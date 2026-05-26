"""Data sink implementations for persisting ingested data."""

import json
import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class DataSink(ABC):
    """Abstract base for all data sinks."""

    def _enrich_items(self, items: list[dict]) -> list[dict]:
        """Add ingestion metadata to each record.

        Injects `ingested_at` (UTC ISO 8601) so downstream consumers
        can track when data entered the pipeline.
        """
        ingested_at = datetime.now(UTC).isoformat()
        return [{**item, "ingested_at": ingested_at} for item in items]

    @abstractmethod
    def write_jsonl(
        self,
        items: list[dict],
        source: str,
        entity_name: str,
        batch_num: int = 0,
    ) -> str:
        """Persist a batch of records in JSONL format.

        Args:
            items: List of dicts to write
            source: Source API name (e.g., 'cen')
            entity_name: Name of the endpoint or entity
            batch_num: Sequential batch number (default 0)

        Returns:
            The path or URI where the data was written
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

    def write_jsonl(
        self,
        items: list[dict],
        source: str,
        entity_name: str,
        batch_num: int = 0,
    ) -> str:
        filename = (
            f"{entity_name}_{batch_num:05d}.jsonl"
            if batch_num > 0
            else f"{entity_name}.jsonl"
        )
        execution_date = datetime.now(UTC).strftime("%Y-%m-%d")
        file_path = self.base_dir / source / execution_date / filename

        file_path.parent.mkdir(parents=True, exist_ok=True)
        enriched = self._enrich_items(items)
        with file_path.open("w") as f:
            for item in enriched:
                f.write(json.dumps(item) + "\n")

        logger.debug("Wrote %d items → %s", len(items), file_path)
        return str(file_path)


class GCSDataSink(DataSink):
    """Write JSONL batches to Google Cloud Storage."""

    def __init__(self, bucket: str, prefix: str = "") -> None:
        """Manage GCS data storage.

        Args:
            bucket: GCS bucket name
            prefix: GCS root prefix (e.g. 'landing', optional)
        """
        import google.auth  # noqa: PLC0415
        from google.cloud import storage as gcs  # noqa: PLC0415

        credentials, project = google.auth.default()
        client = gcs.Client(credentials=credentials, project=project)
        self.bucket = client.bucket(bucket)
        self.prefix = prefix

    def write_jsonl(
        self,
        items: list[dict],
        source: str,
        entity_name: str,
        batch_num: int = 0,
    ) -> str:
        filename = (
            f"{entity_name}_{batch_num:05d}.jsonl"
            if batch_num > 0
            else f"{entity_name}.jsonl"
        )
        execution_date = datetime.now(UTC).strftime("%Y-%m-%d")
        parts = [p for p in (self.prefix, source, execution_date, filename) if p]
        blob_path = "/".join(parts)
        enriched = self._enrich_items(items)
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string("\n".join(json.dumps(i) for i in enriched))

        uri = f"gs://{self.bucket.name}/{blob_path}"
        logger.debug("Uploaded %d items → %s", len(items), uri)
        return uri
