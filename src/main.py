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
from image_deduper import ImageDeduper
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
image_deduper = ImageDeduper(config.found_offers_file.with_name("found_offer_image_hashes.json"))
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
    title = title.replace("\xa0", " ").replace(",", "")  # nezlomitelny mezernik
    split = title.split(" ")
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


@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.channel_id != config.discord.offers_channel:
        return

    if client.user is not None and payload.user_id == client.user.id:
        return

    source_channel = client.get_channel(payload.channel_id)
    if source_channel is None:
        source_channel = await client.fetch_channel(payload.channel_id)

    target_channel = client.get_channel(config.discord.repost_channel)
    if target_channel is None:
        target_channel = await client.fetch_channel(config.discord.repost_channel)

    source_message = await source_channel.fetch_message(payload.message_id)
    if not source_message.embeds:
        logging.info(f"Reaction repost skipped because message {payload.message_id} has no embeds.")
        return

    embed = source_message.embeds[0].copy()
    emoji_prefix = str(payload.emoji)
    embed.title = f"{emoji_prefix} {embed.title}"

    await retry_until_successful_send(target_channel, embed)

    logging.info(f"Offer reposted from message_id={payload.message_id} to channel_id={target_channel.id} with emoji={emoji_prefix}")

@tasks.loop(minutes=interval_time)
async def process_latest_offers():
    logging.info("Fetching offers")

    new_offers: list[RentalOffer] = []
    latest_offers = await asyncio.to_thread(fetch_latest_offers, scrapers)
    await asyncio.to_thread(image_deduper.cleanup_expired)
    for offer in latest_offers:
        if not storage.contains(offer):
            is_unique_image = await asyncio.to_thread(image_deduper.accept_offer, offer.image_url)
            if is_unique_image:
                new_offers.append(offer)

    first_time = storage.first_time
    storage.save_offers(new_offers)

    logging.info("Offers fetched (new: {})".format(len(new_offers)))

    if not first_time:
        for offer in new_offers:
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

            await retry_until_successful_send(channel, embed)
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


async def retry_until_successful_send(channel: discord.TextChannel, embed: discord.Embed, delay: float = 5.0):
    """Retry sending a message with one embed until it succeeds."""
    while True:
        try:
            await channel.send(embed=embed)
            logging.info("Embed successfully sent.")
            return
        except discord.errors.DiscordServerError as e:
            logging.warning(f"Discord server error while sending embed: {e}. Retrying in {delay:.1f}s.")
        except discord.errors.HTTPException as e:
            logging.warning(f"HTTPException while sending embed: {e}. Retrying in {delay:.1f}s.")
        except Exception as e:
            logging.exception(f"Unexpected error while sending embed: {e}. Retrying in {delay:.1f}s.")
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
