import logging
import os
import sys

from dotenv import load_dotenv
from shared.base_client import PaginatedAPIClient
from shared.storage import GCSDataSink, LocalDataSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _send_to_local(data: list[dict], batch_num: int, start_date: str) -> None:
    base_dir = "data/raw/cen_api/"
    sink = LocalDataSink(base_dir)

    logger.info("Configured LocalDataSink to %s", base_dir)
    sink.append_jsonl(data, batch_num, start_date)


def _send_to_gcs(data: list[dict], batch_num: int, start_date: str) -> None:
    sink = GCSDataSink(
        bucket=_require_env("GCS_BUCKET"), prefix=_require_env("GCS_PREFIX")
    )
    sink.append_jsonl(data, batch_num, start_date)


_SINKS = {
    "local": _send_to_local,
    "gcs": _send_to_gcs,
}


def main() -> None:
    load_dotenv()

    # project_id = _require_env("GCP_PROJECT_ID")
    endpoint = _require_env("ENDPOINT")
    base_url = _require_env("BASE_URL")
    auth_token = _require_env("SECRET_AUTH_TOKEN")
    start_date = _require_env("START_DATE")
    end_date = _require_env("END_DATE")
    id_central = _require_env("ID_CENTRAL")
    page_size = int(os.getenv("PAGE_SIZE", "1000"))
    sleep = float(os.getenv("SLEEP", "2.0"))
    max_pages = int(os.getenv("MAX_PAGES", "0")) or 1
    sink_target = os.getenv("SINK", "gcs")  # "gcs" | "local"

    if sink_target not in _SINKS:
        raise ValueError(f"Unknown SINK '{sink_target}'. Choose from: {list(_SINKS)}")

    # -- Client -------------------------------------------------------------
    client = PaginatedAPIClient(base_url=base_url)

    headers = {"accept": "application/json"}
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "idCentral": id_central,
        "user_key": auth_token,
    }

    # -- Paginate & persist -------------------------------------------------
    page_generator = client.get_offset_pages(
        endpoint=endpoint,
        params=params,
        results_key="data",
        headers=headers,
        sleep=sleep,
        page_size=page_size,
        max_pages=max_pages,
        strategy_param="page",
        limit_param="pageSize",
    )

    save = _SINKS[sink_target]

    for batch_num, page in enumerate(page_generator, start=1):
        if not page:
            logger.warning("Received empty page at batch %d. Stopping.", batch_num)
            break

        logger.info(
            "Saving batch %d (%d items) → %s", batch_num, len(page), sink_target
        )
        save(page, batch_num, start_date)

    logger.info("Job finished.")


if __name__ == "__main__":
    main()
