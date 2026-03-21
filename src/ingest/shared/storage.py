"""Data sink implementations for persisting ingested data."""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from google.cloud import storage as gcs

logger = logging.getLogger(__name__)


def hive_partition_path(date_str: str) -> str:
    """Build Hive-style partition path from a date string (YYYY-MM-DD).

    Args:
        date_str: Date in YYYY-MM-DD format
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


class DataSink(ABC):
    """Abstract base for all data sinks."""

    @abstractmethod
    def append_jsonl(self, items: list[dict], batch_num: int, date_str: str) -> None:
        """Persist a batch of records in JSONL format.

        Args:
            items: List of dicts to write
            batch_num: Sequential batch number
            date_str: Date string used for partitioning (YYYY-MM-DD)
        """


class LocalDataSink(DataSink):
    """Write JSONL batches to the local filesystem."""

    def __init__(self, base_dir: str) -> None:
        """Manage local data storage.

        Args:
            base_dir: Local directory to store data
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def append_jsonl(self, items: list[dict], batch_num: int, date_str: str) -> None:
        partition = hive_partition_path(date_str)
        file_path = self.base_dir / partition / f"batch_{batch_num:05d}.jsonl"

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")

        logger.debug("Wrote %d items → %s", len(items), file_path)


class GCSDataSink(DataSink):
    """Write JSONL batches to Google Cloud Storage."""

    def __init__(self, bucket: str, prefix: str) -> None:
        """Manage GCS data storage.

        Args:
            bucket: GCS bucket name
            prefix: GCS prefix (e.g. ``raw/cen_api``)
        """
        self.bucket = gcs.Client().bucket(bucket)
        self.prefix = prefix

    def append_jsonl(self, items: list[dict], batch_num: int, date_str: str) -> None:
        partition = hive_partition_path(date_str)
        blob_path = f"{self.prefix}/{partition}/batch_{batch_num:05d}.jsonl"
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string("\n".join(json.dumps(i) for i in items))

        logger.debug(
            "Uploaded %d items → gs://%s/%s",
            len(items),
            self.bucket.name,
            blob_path,
        )
