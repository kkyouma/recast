"""ERA5 historical weather data collector (via CDS API)."""

import logging
import sys
import tempfile
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import polars as pl

from shared.config import get_env
from shared.storage import DataSink, GCSDataSink, LocalDataSink

log_level = get_env("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

ENERGY_PRESETS = {
    "solar": [
        "surface_solar_radiation_downwards",  # GHI (Driver principal de generación)
        "total_sky_direct_solar_radiation_at_surface",  # DNI (Para trackers/seguidores)
        "2m_temperature",  # Afecta la eficiencia térmica del panel
        "total_cloud_cover",  # Captura paso de nubes y rampas de caída
    ],
    "wind": [
        "100m_u_component_of_wind",  # Velocidad zonal a altura de buje
        "100m_v_component_of_wind",  # Velocidad meridional a altura de buje
        "10m_u_component_of_wind",  # Útil para calcular cizalladura (wind shear)
        "10m_v_component_of_wind",  # Útil para calcular cizalladura (wind shear)
        "surface_pressure",  # Necesaria para calcular densidad del aire
        "2m_temperature",  # Necesaria para calcular densidad del aire
        "10m_wind_gust_since_previous_post_processing",  # Alertas cut-out
    ],
}


class ERA5Collector:
    """Client for collecting historical climate data from ERA5 via CDS API."""

    def __init__(self, sink: DataSink) -> None:
        """Initialize the ERA5 collector.

        Args:
            sink: Data sink for saving extracted JSONL batches.
        """
        try:
            import cdsapi  # noqa: PLC0415
        except ImportError:
            logger.exception("cdsapi is not installed.")
            raise

        self.client = cdsapi.Client()
        self.sink = sink
        self.dataset = "reanalysis-era5-single-levels-timeseries"

    def collect(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        variables: Sequence[str] | Literal["solar", "wind"] = "solar",
    ) -> str:
        """Fetch and store ERA5 climate data for specific coordinates.

        Args:
            latitude: Target latitude
            longitude: Target longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            variables: List of variables to fetch. Defaults to energy prediction set.

        Returns:
            The URI where the processed JSONL data was saved.
        """
        target_vars = (
            ENERGY_PRESETS[variables] if isinstance(variables, str) else variables
        )

        request_params = {
            "data_format": "csv",
            "variable": list(target_vars),
            "location": {"longitude": longitude, "latitude": latitude},
            "date": [f"{start_date}/{end_date}"],
        }

        # Create a temporary directory for the download
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "era5_data.zip"

            logger.info(
                "Requesting ERA5 data for coordinates (%s, %s) from %s to %s",
                latitude,
                longitude,
                start_date,
                end_date,
            )

            self.client.retrieve(self.dataset, request_params, str(zip_path))

            logger.debug("Successfully downloaded ZIP to %s. Extracting...", zip_path)

            with zipfile.ZipFile(zip_path, "r") as z:
                # The CDS API 'csv' format typically returns a zip containing one CSV
                csv_filename = next(
                    name for name in z.namelist() if name.endswith(".csv")
                )
                extracted_path = z.extract(csv_filename, path=tmpdir)

            logger.debug(
                "Successfully extracted CSV to %s. Converting to JSONL...",
                extracted_path,
            )

            # Read with polars (fast and efficient CSV parsing)
            df = pl.read_csv(extracted_path)

            # Convert to list of dictionaries
            # This is efficient enough for small payloads like single coordinates
            items = df.to_dicts()

            entity_name = f"era5_lat{latitude}_lon{longitude}"

            # Save using the data sink
            uri = self.sink.write_jsonl(
                items=items,
                source="era5",
                entity_name=entity_name,
            )

            logger.info("Successfully persisted ERA5 data to %s", uri)
            return uri


def _build_sink(sink_target: str) -> DataSink:
    """Instantiate the appropriate DataSink (once per job run).

    Args:
        sink_target: "local" or "gcs"
    """
    if sink_target == "local":
        base_dir = get_env("LOCAL_BASE_DIR", "data/raw/")
        logger.info("Configured LocalDataSink → %s", base_dir)
        return LocalDataSink(base_dir=base_dir)

    if sink_target == "gcs":
        # We need require_env from config, but we can just use get_env with a fallback
        # Let's import require_env at the top or just use get_env and raise if missing
        from shared.config import require_env  # noqa: PLC0415

        bucket = require_env("GCS_BUCKET")
        prefix = get_env("GCS_PREFIX", "")

        sink_path = f"gs://{bucket}/{prefix}" if prefix else f"gs://{bucket}"
        logger.info("Configured GCSDataSink → %s", sink_path)
        return GCSDataSink(bucket=bucket, prefix=prefix)

    raise ValueError(f"Unknown SINK '{sink_target}'. Choose from: local, gcs")


def main() -> None:
    """Job entrypoint — fetches ERA5 historical weather data."""
    latitude = float(get_env("LATITUDE", "0.0"))
    longitude = float(get_env("LONGITUDE", "0.0"))
    start_date = get_env("START_DATE", "2026-01-01")
    end_date = get_env("END_DATE", "2026-01-02")

    sink_target = get_env("SINK", "gcs")
    sink = _build_sink(sink_target)

    collector = ERA5Collector(sink=sink)

    uri = collector.collect(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info("Job finished successfully. Output URI: %s", uri)
