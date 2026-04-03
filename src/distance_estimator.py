import logging
import time

import requests

from scrapers.rental_offer import RentalOffer


class DistanceEstimator:
    """Estimate distance from a fixed origin to an offer in meters.

    Uses free public APIs:
    - Nominatim (OpenStreetMap) for geocoding textual address to coordinates
    - OSRM demo server for route distance estimate
    """

    def __init__(self, origin_lat: float, origin_lon: float):
        self.origin_lat = origin_lat
        self.origin_lon = origin_lon

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "web-scraper-nabidek-pronajmu/1.0 (discord distance estimator)",
        })

        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._distance_cache: dict[tuple[float, float], int | None] = {}
        self._last_geocode_request_at = 0.0

    def estimate_distance_meters(self, offer: RentalOffer) -> int | None:
        coords = self._get_offer_coordinates(offer)
        if coords is None:
            coords = self._geocode_address(offer.location)

        if coords is None:
            return None

        rounded_coords = (round(coords[0], 6), round(coords[1], 6))
        if rounded_coords in self._distance_cache:
            return self._distance_cache[rounded_coords]

        distance = self._fetch_route_distance(coords[0], coords[1])
        self._distance_cache[rounded_coords] = distance
        return distance

    @staticmethod
    def _get_offer_coordinates(offer: RentalOffer) -> tuple[float, float] | None:
        if offer.latitude is None or offer.longitude is None:
            return None
        return (offer.latitude, offer.longitude)

    def _geocode_address(self, address: str) -> tuple[float, float] | None:
        if address in self._geocode_cache:
            return self._geocode_cache[address]

        try:
            # Public Nominatim usage policy requires low request rate.
            elapsed = time.monotonic() - self._last_geocode_request_at
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

            response = self._session.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": f"{address}, Czechia",
                    "format": "jsonv2",
                    "limit": 1,
                    "countrycodes": "cz",
                },
                timeout=10,
            )
            self._last_geocode_request_at = time.monotonic()
            response.raise_for_status()
            payload = response.json()
            if not payload:
                self._geocode_cache[address] = None
                return None

            coords = (float(payload[0]["lat"]), float(payload[0]["lon"]))
            self._geocode_cache[address] = coords
            return coords
        except Exception as exc:
            logging.debug("Geocoding failed for '%s': %s", address, exc)
            self._geocode_cache[address] = None
            return None

    def _fetch_route_distance(self, destination_lat: float, destination_lon: float) -> int | None:
        try:
            response = self._session.get(
                "https://router.project-osrm.org/route/v1/driving/"
                f"{self.origin_lon},{self.origin_lat};{destination_lon},{destination_lat}",
                params={"overview": "false"},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()

            if payload.get("code") != "Ok" or not payload.get("routes"):
                return None

            return int(round(payload["routes"][0]["distance"]))
        except Exception as exc:
            logging.debug("OSRM distance lookup failed: %s", exc)
            return None
