#!/usr/bin/evn python3
import json
import logging
from datetime import datetime, timedelta, timezone
from time import time
from pathlib import Path
import unicodedata
from zoneinfo import ZoneInfo

import discord
from discord.ext import tasks

from config import *
from distance_estimator import DistanceEstimator
from discord_logger import DiscordLogger
from offers_storage import OffersStorage
from scrapers.rental_offer import RentalOffer
from scrapers_manager import create_scrapers, fetch_latest_offers
import asyncio

def get_current_time() -> datetime:
    return datetime.now(ZoneInfo("Europe/Prague"))

def get_current_daytime() -> bool: 
    brno_time = get_current_time()
    
    return brno_time.hour in range(6, 22)


client = discord.Client(intents=discord.Intents.default())

daytime = get_current_daytime()
interval_time = config.refresh_interval_daytime_minutes if daytime else config.refresh_interval_nighttime_minutes

scrapers = create_scrapers(config.dispositions)
landmark_estimators = {
    "Vzdialenosť k FI MU": DistanceEstimator(origin_lat=49.2098333, origin_lon=16.599),
    "Vzdialenosť k Náměstí svobody": DistanceEstimator(origin_lat=49.1951389, origin_lon=16.6080278),
    "Vzdialenosť od Cejlu 💀": DistanceEstimator(origin_lat=49.1985278, origin_lon=16.6207778),
}

bad_streets_file = Path(__file__).resolve().parent / "data" / "bad_streets.json"
with bad_streets_file.open(encoding="utf-8") as file:
    bad_streets = json.load(file)

async def set_activity(message: str):
    custom_activity = discord.CustomActivity(name=message)
    await client.change_presence(activity=custom_activity)

def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(character for character in normalized if not unicodedata.combining(character)).casefold()

def sanitize_price(price: int | str) -> int:
    if isinstance(price, int):
        return price
    if isinstance(price, str) and price.strip().isdecimal():
        return int(price.strip())
    if isinstance(price, str) and "/" in price:
        parts = price.split("/")
        return sum(sanitize_price(part) for part in parts)
    return 0

def get_bad_streets(offer: RentalOffer) -> list[str]:
    normalized_location = normalize_text(offer.location)
    return [street for street in bad_streets if normalize_text(street) in normalized_location]


def format_distance(distance_meters: int) -> str:
    if distance_meters < 1000:
        return f"{distance_meters} m"
    return f"{distance_meters / 1000:.1f} km"

def get_price_per_sqm(title: str, price: int) -> int | None:
    logging.info(title)
    title = title.replace("\xa0", " ")  # nezlomitelny mezernik
    split = title.split(" ")
    logging.info(split)
    if "m²" in split:
        size = split[split.index("m²") - 1]
        if size.isdecimal():
            return int(price / int(size))
    return None

async def estimate_landmark_distances(offer: RentalOffer) -> dict[str, int | None]:
    tasks = [
        asyncio.to_thread(estimator.estimate_distance_meters, offer)
        for estimator in landmark_estimators.values()
    ]
    values = await asyncio.gather(*tasks)
    return dict(zip(landmark_estimators.keys(), values))

@client.event
async def on_ready():
    global channel, storage

    dev_channel = client.get_channel(config.discord.dev_channel)
    channel = client.get_channel(config.discord.offers_channel)
    storage = OffersStorage(config.found_offers_file)

    if not config.debug:
        discord_error_logger = DiscordLogger(client, dev_channel, logging.ERROR)
        logging.getLogger().addHandler(discord_error_logger)
    else:
        logging.info("Discord logger is inactive in debug mode")

    logging.info("Available scrapers: " + ", ".join([s.name for s in scrapers]))

    logging.info("Fetching latest offers every {} minutes".format(interval_time))

    process_latest_offers.start()

@tasks.loop(minutes=interval_time)
async def process_latest_offers():
    logging.info("Fetching offers")

    new_offers: list[RentalOffer] = []
    latest_offers = await asyncio.to_thread(fetch_latest_offers, scrapers)
    for offer in latest_offers:
        if not storage.contains(offer):
            new_offers.append(offer)

    first_time = storage.first_time
    storage.save_offers(new_offers)

    logging.info("Offers fetched (new: {})".format(len(new_offers)))

    if not first_time:
        def chunk_offers(offers, size):
            for i in range(0, len(offers), size):
                yield offers[i:i + size]

        for offer_batch in chunk_offers(new_offers, config.embed_batch_size):
            embeds = []

            for offer in offer_batch:
                landmark_distances = await estimate_landmark_distances(offer)

                embed = discord.Embed(
                    title=offer.title,
                    url=offer.link,
                    description=offer.location,
                    timestamp=datetime.now(timezone.utc),
                    color=offer.scraper.color
                )
                embed.add_field(name="Cena", value=str(offer.price) + " Kč")
                price_per_sqm = get_price_per_sqm(offer.title, sanitize_price(offer.price))
                embed.add_field(name="Cena/m²", value=(str(price_per_sqm) + " Kč" if price_per_sqm is not None else "Nedostupné"))
                for label, distance_meters in landmark_distances.items():
                    embed.add_field(
                        name=label,
                        value=(format_distance(distance_meters) if distance_meters is not None else "Nedostupné")
                    )
                bad_streets_for_offer = get_bad_streets(offer)
                if bad_streets_for_offer:
                    embed.add_field(name="⚠️ Zlá ulica", value=", ".join(bad_streets_for_offer), inline=False)
                embed.set_author(name=offer.scraper.name, icon_url=offer.scraper.logo_url)
                embed.set_image(url=offer.image_url)

                embeds.append(embed)

            await retry_until_successful_send(channel, embeds)
            await asyncio.sleep(1.5)
    else:
        logging.info("No previous offers, first fetch is running silently")

    global daytime, interval_time
    if daytime != get_current_daytime():  # Pokud stary daytime neodpovida novemu

        daytime = not daytime  # Zneguj daytime (podle podminky se zmenil)

        interval_time = config.refresh_interval_daytime_minutes if daytime else config.refresh_interval_nighttime_minutes

        logging.info("Fetching latest offers every {} minutes".format(interval_time))
        process_latest_offers.change_interval(minutes=interval_time)

    await retry_until_successful_edit(channel, f"Last update <t:{int(time())}:R>")
    await set_activity(f"Nejbližší aktualizace o {get_current_time() + timedelta(minutes=interval_time):%H:%M}")


async def retry_until_successful_send(channel: discord.TextChannel, embeds: list[discord.Embed], delay: float = 5.0):
    """Retry sending a message with embeds until it succeeds."""
    while True:
        try:
            await channel.send(embeds=embeds)
            logging.info("Embeds successfully sent.")
            return
        except discord.errors.DiscordServerError as e:
            logging.warning(f"Discord server error while sending embeds: {e}. Retrying in {delay:.1f}s.")
        except discord.errors.HTTPException as e:
            logging.warning(f"HTTPException while sending embeds: {e}. Retrying in {delay:.1f}s.")
        except Exception as e:
            logging.exception(f"Unexpected error while sending embeds: {e}. Retrying in {delay:.1f}s.")
            raise e
        await asyncio.sleep(delay)


async def retry_until_successful_edit(channel: discord.TextChannel, topic: str, delay: float = 5.0):
    """Retry editing a channel topic until it succeeds."""
    while True:
        try:
            await channel.edit(topic=topic)
            logging.info(f"Channel topic successfully updated to: {topic}")
            return
        except discord.errors.DiscordServerError as e:
            logging.warning(f"Discord server error while editing topic: {e}. Retrying in {delay:.1f}s.")
        except discord.errors.HTTPException as e:
            logging.warning(f"HTTPException while editing topic: {e}. Retrying in {delay:.1f}s.")
        except Exception as e:
            logging.exception(f"Unexpected error while editing channel topic: {e}. Retrying in {delay:.1f}s.")
            raise e
        await asyncio.sleep(delay)

if __name__ == "__main__":
    logging.basicConfig(
        level=(logging.DEBUG if config.debug else logging.INFO),
        format='%(asctime)s - [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug("Running in debug mode")

    client.run(config.discord.token, log_level=logging.INFO)
