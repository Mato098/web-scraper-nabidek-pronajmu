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
    image_url: str | None = None
    phash_value: str | None = None


class ImageDeduper:
    def __init__(
        self,
        path: str | Path,
        retention: timedelta = timedelta(hours=1),
        max_distance: float = 1.25,
        max_phash_distance: int = 4,
    ):
        self.path = Path(path)
        self.retention = retention
        self.max_distance = max_distance
        self.max_phash_distance = max_phash_distance
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
        hashes = self._download_and_hashes(image_url)
        if hashes is None:
            return True

        image_hash, image_phash = hashes

        for record in self._records:
            existing_hash = imagehash.hex_to_multihash(record.hash_value)
            crop_distance = image_hash - existing_hash

            if crop_distance > self.max_distance:
                continue

            if record.phash_value:
                existing_phash = imagehash.hex_to_hash(record.phash_value)
                phash_distance = image_phash - existing_phash
                if phash_distance > self.max_phash_distance:
                    continue

                logging.info(
                    f"Duplicate image detected, crop distance {crop_distance} <= {self.max_distance}, "
                    f"phash distance {phash_distance} <= {self.max_phash_distance}:\n"
                    f"NEW URL: {image_url}\n"
                    f"OLD URL: {record.image_url or '<unknown>'}"
                )
                return False

            if crop_distance == 0:
                logging.info(
                    f"Duplicate image detected (legacy record), crop distance {crop_distance} == 0:\n"
                    f"NEW URL: {image_url}\n"
                    f"OLD URL: {record.image_url or '<unknown>'}"
                )
                return False

        self._records.append(
            _HashRecord(
                hash_value=str(image_hash),
                seen_at=datetime.now(timezone.utc),
                image_url=image_url,
                phash_value=str(image_phash),
            )
        )
        self._save_records()
        return True

    def _download_and_hashes(self, image_url: str) -> tuple[imagehash.ImageMultiHash, imagehash.ImageHash] | None:
        if not image_url:
            return None

        try:
            response = self._session.get(image_url, timeout=15)
            response.raise_for_status()

            with Image.open(BytesIO(response.content)) as image:
                rgb_image = image.convert("RGB")
                return imagehash.crop_resistant_hash(rgb_image), imagehash.phash(rgb_image)
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
                        image_url=item.get("image_url"),
                        phash_value=item.get("phash"),
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
                "image_url": record.image_url,
                "phash": record.phash_value,
            }
            for record in self._records
        ]

        with self.path.open("w", encoding="utf-8") as file_object:
            json.dump(payload, file_object, ensure_ascii=False, indent=2)