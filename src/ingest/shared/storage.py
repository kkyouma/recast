# storage.py
import json
from datetime import datetime
from pathlib import Path

from google.cloud import storage as gcs


class LocalDataSink:
    def __init__(self, base_dir: str) -> None:
        """Manage local data storage.

        Args:
            base_dir: Local directory to store data
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _hive_partition_path(self, date_str: str) -> str:
        """Build Hive-style partition path from a date string (YYYY-MM-DD)."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

    def append_jsonl(self, items: list[dict], batch_num: int, date_str: str) -> None:
        partition = self._hive_partition_path(date_str)
        file_path = self.base_dir / partition / f"batch_{batch_num:05d}.jsonl"

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")


class GCSDataSink:
    def __init__(self, bucket: str, prefix: str) -> None:
        """Manage GCS data storage.

        Args:
            bucket: GCS bucket name
            prefix: GCS prefix
        """
        self.bucket = gcs.Client().bucket(bucket)
        self.prefix = prefix

    def _hive_partition_path(self, date_str: str) -> str:
        """Build Hive-style partition path from a date string (YYYY-MM-DD)."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

    def append_jsonl(self, items: list[dict], batch_num: int, date_str: str) -> None:
        partition = self._hive_partition_path(date_str)
        blob = self.bucket.blob(
            f"{self.prefix}/{partition}/batch_{batch_num:05d}.jsonl"
        )
        blob.upload_from_string("\n".join(json.dumps(i) for i in items))

    # TODO: Add support for csv format
    def append_csv(
        self, items: list[dict], batch_num: int, start_date: str
    ) -> None: ...
