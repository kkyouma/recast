"""CEN API collector — Cloud Run Job entrypoint.

A single Docker image built from this module serves as a template for
all CEN (Coordinador Eléctrico Nacional) API endpoints.  Each Cloud Run
Job overrides only the environment variables it needs.

Configure entirely via environment variables:

  BASE_URL          API base URL
  ENDPOINT          API endpoint path
  SECRET_AUTH_TOKEN  API token (env var or GCP Secret Manager)

  START_DATE        Query param: start date (YYYY-MM-DD)
  END_DATE          Query param: end date (YYYY-MM-DD)
  EXTRA_PARAMS      Optional JSON with additional query params
                    e.g. '{"idCentral": "464"}'

  PAGE_SIZE         Items per page  (default 1000)
  SLEEP             Seconds between requests (default 2.0)
  MAX_PAGES         Max pages to fetch (empty = unlimited)

  RESULTS_KEY       Response key containing items (default "data")
  STRATEGY_PARAM    Page param name (default "page")
  LIMIT_PARAM       Page size param name (default "limit")

  SINK              "gcs" | "local"  (default "gcs")
  LOCAL_BASE_DIR    Local directory when SINK=local (default "data/raw/")
  GCS_BUCKET        GCS bucket name   (required if SINK=gcs)
  GCS_PREFIX        GCS prefix         (required if SINK=gcs)
"""

import json
import logging
import sys

from shared.base_client import PaginatedAPIClient
from shared.config import require_env
from shared.secrets import get_secret
from shared.storage import DataSink, GCSDataSink, LocalDataSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _build_sink(sink_target: str) -> DataSink:
    """Instantiate the appropriate DataSink (once per job run).

    Args:
        sink_target: "local" or "gcs"
    """
    if sink_target == "local":
        base_dir = require_env("LOCAL_BASE_DIR", "data/raw/")
        logger.info("Configured LocalDataSink → %s", base_dir)
        return LocalDataSink(base_dir=base_dir)

    if sink_target == "gcs":
        bucket = require_env("GCS_BUCKET")
        prefix = require_env("GCS_PREFIX")
        logger.info("Configured GCSDataSink → gs://%s/%s", bucket, prefix)
        return GCSDataSink(bucket=bucket, prefix=prefix)

    raise ValueError(f"Unknown SINK '{sink_target}'. Choose from: local, gcs")


def main() -> None:
    """Job entrypoint — reads env vars, paginates, and persists."""
    # -- Connection ----------------------------------------------------------
    endpoint = require_env("ENDPOINT", "/generacion-real/v3/findByDate")
    base_url = require_env("BASE_URL", "https://sipub.api.coordinador.cl:443")
    auth_token = get_secret("SECRET_AUTH_TOKEN")

    # -- Query params --------------------------------------------------------
    start_date = require_env("START_DATE", "2026-01-01")
    end_date = require_env("END_DATE", "2026-01-02")

    extra_params_raw = require_env("EXTRA_PARAMS", '{"idCentral": "464"}')
    extra_params: dict = json.loads(extra_params_raw)

    # -- Pagination settings -------------------------------------------------
    page_size = int(require_env("PAGE_SIZE", "5000"))
    sleep = float(require_env("SLEEP", "2.0"))

    max_pages_raw = int(require_env("MAX_PAGES", "3"))
    max_pages = int(max_pages_raw) if max_pages_raw else None

    results_key = require_env("RESULTS_KEY", "data")
    strategy_param = require_env("STRATEGY_PARAM", "page")
    limit_param = require_env("LIMIT_PARAM", "limit")

    # -- Sink ----------------------------------------------------------------
    sink_target = require_env("SINK", "gcs")
    sink = _build_sink(sink_target)

    # -- Client --------------------------------------------------------------
    client = PaginatedAPIClient(base_url=base_url)

    params: dict = {
        "startDate": start_date,
        "endDate": end_date,
        "user_key": auth_token,
        **extra_params,
    }

    # -- Paginate & persist --------------------------------------------------
    page_generator = client.get_offset_pages(
        endpoint=endpoint,
        params=params,
        results_key=results_key,
        headers={"accept": "application/json"},
        sleep=sleep,
        page_size=page_size,
        max_pages=max_pages,
        strategy_param=strategy_param,
        limit_param=limit_param,
    )

    for batch_num, page in enumerate(page_generator, start=1):
        if not page:
            logger.warning("Received empty page at batch %d. Stopping.", batch_num)
            break

        logger.info(
            "Saving batch %d (%d items) → %s",
            batch_num,
            len(page),
            sink_target,
        )
        sink.append_jsonl(page, batch_num, start_date)

    logger.info("Job finished.")


if __name__ == "__main__":
    main()
