from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
import imagehash
import requests
from PIL import Image


@dataclass(slots=True)
class _HashRecord:
    hash_value: str
    seen_at: datetime


class ImageDeduper:
    def __init__(self, path: str | Path, retention: timedelta = timedelta(hours=3), max_distance: int = 8):
        self.path = Path(path)
        self.retention = retention
        self.max_distance = max_distance
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "web-scraper-nabidek-pronajmu/1.0 (image deduper)",
        })
        self._records: list[_HashRecord] = self._load_records()

    def cleanup_expired(self) -> None:
        now = datetime.now(timezone.utc)
        cutoff = now - self.retention
        kept_records = [record for record in self._records if record.seen_at >= cutoff]

        if len(kept_records) == len(self._records):
            return

        self._records = kept_records
        self._save_records()

    def accept_offer(self, image_url: str) -> bool:
        image_hash = self._download_and_hash(image_url)
        if image_hash is None:
            return True

        for record in self._records:
            existing_hash = imagehash.hex_to_multihash(record.hash_value)
            if image_hash - existing_hash <= self.max_distance:
                logging.info("Duplicate image detected (distance %d <= %d) for URL: %s", image_hash - existing_hash, self.max_distance, image_url)
                return False

        self._records.append(_HashRecord(hash_value=str(image_hash), seen_at=datetime.now(timezone.utc)))
        self._save_records()
        return True

    def _download_and_hash(self, image_url: str) -> imagehash.ImageHash | None:
        if not image_url:
            return None

        try:
            response = self._session.get(image_url, timeout=15)
            response.raise_for_status()

            with Image.open(BytesIO(response.content)) as image:
                return imagehash.crop_resistant_hash(image.convert("RGB"))
        except Exception:
            logging.debug("Image hashing failed for %s", image_url, exc_info=True)
            return None

    def _load_records(self) -> list[_HashRecord]:
        try:
            with self.path.open(encoding="utf-8") as file_object:
                payload = json.load(file_object)
        except FileNotFoundError:
            return []
        except Exception:
            logging.warning("Could not load image hash store from %s", self.path, exc_info=True)
            return []

        records: list[_HashRecord] = []
        for item in payload:
            try:
                records.append(
                    _HashRecord(
                        hash_value=item["hash"],
                        seen_at=datetime.fromisoformat(item["seen_at"]),
                    )
                )
            except Exception:
                logging.debug("Skipping invalid image hash record: %s", item, exc_info=True)

        return records

    def _save_records(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "hash": record.hash_value,
                "seen_at": record.seen_at.isoformat(),
            }
            for record in self._records
        ]

        with self.path.open("w", encoding="utf-8") as file_object:
            json.dump(payload, file_object, ensure_ascii=False, indent=2)