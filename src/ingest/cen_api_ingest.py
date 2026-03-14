import json
import logging
import os

import polars as pl
from client import PaginatedAPIClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

URL_BASE = os.getenv("BASE_URL")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")


def get_generation_data(
    client: PaginatedAPIClient,
    start_date: str,
    end_date: str,
    api_key: str,
    id_central: str | None = None,
):
    headers = {"accept": "application/json"}
    endpoint = "generacion-real/v3/findByDate"
    query_params = {
        "startDate": start_date,
        "endDate": end_date,
        "idCentral": id_central or "",
        "user_key": api_key,
        # "pageSize": "200",
    }

    return client.get_offset_pages(
        endpoint=endpoint,
        params=query_params,
        headers=headers,
        sleep=2.0,
        page_size=100,
        max_pages=50,
    )


if __name__ == "__main__":
    if URL_BASE is None or AUTH_TOKEN is None:
        raise ValueError("Environment variables BASE_URL and AUTH_TOKEN are required.")

    paginated_client = PaginatedAPIClient(base_url=URL_BASE)

    response = get_generation_data(
        client=paginated_client,
        start_date="2025-02-01",
        end_date="2025-04-28",
        id_central="464",
        api_key=AUTH_TOKEN,
    )

    print(json.dumps(response, indent=4))

    df = pl.DataFrame(response["data"])
    df.write_csv("./cen.csv")
