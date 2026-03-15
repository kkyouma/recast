import logging
import time
from collections.abc import Generator
from typing import Any

import requests

logger = logging.getLogger(__name__)


class APIClient:
    """Base class for API clients."""

    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _build_url(self, endpoint: str) -> str:
        """Construct full URL."""
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        url = self._build_url(endpoint)

        logger.debug(
            "API request: %s %s params=%s headers=%s",
            method,
            url,
            params,
            headers,
        )

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=json_data,
                timeout=self.timeout,
            )

            logger.debug("Response status: %s", response.status_code)

            response.raise_for_status()

            return response.json()

        except requests.exceptions.Timeout:
            logger.exception("Request timeout for %s", url)
            raise

        except requests.exceptions.ConnectionError:
            logger.exception("Connection error for %s", url)
            raise

        except requests.exceptions.HTTPError:
            logger.exception(
                "HTTP error %s for %s | response=%s",
                response.status_code,
                url,
                response.text[:500],
            )
            raise

        except ValueError:
            logger.exception("JSON decode error for %s", url)
            raise

    def get(self, endpoint: str, **kwargs) -> dict[str, Any]:
        return self._request("GET", endpoint, **kwargs)

    def post(
        self,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        return self._request("POST", endpoint, json_data=json_data, **kwargs)

    def patch(self, endpoint: str, **kwargs) -> dict[str, Any]:
        return self._request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> None:
        self._request("DELETE", endpoint, **kwargs)


class PaginatedAPIClient(APIClient):
    """Extends APIClient to handle pagination."""

    def get_offset_pages(  # noqa: PLR0913
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str | None = "data",
        page_size: int = 50,
        max_pages: int = 100,
        strategy_param: str = "page",
        limit_param: str = "pageSize",
        sleep: float = 0.1,
    ) -> Generator:
        offset = 0
        base_params = {**(params or {}), limit_param: page_size}

        for page in range(1, max_pages + 1):
            page_params = {**base_params, strategy_param: page}
            logger.info(
                "Fetching page %d | offset=%d | endpoint=%s",
                page,
                offset,
                endpoint,
            )

            response = self.get(endpoint, params=page_params, headers=headers)

            if isinstance(response, list):
                items = response
            else:
                items = response.get(results_key, []) if results_key else response
            logger.debug(
                "Received %d items",
                len(items),
            )
            yield items

            if len(items) < page_size:
                logger.info("Pagination finished after %d pages", page)
                break

            offset += page_size

            time.sleep(sleep)

        else:
            logger.warning("Reached max_pages=%d for %s", max_pages, endpoint)

    def get_cursor_pages(  # noqa: PLR0913
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str | None = "results",
        cursor_key: str = "next_cursor",
        cursor_param: str = "cursor",
        max_pages: int = 100,
        sleep: float = 0.1,
    ) -> Generator:

        results: list[dict[str, Any]] = []
        params = params or {}

        for page in range(1, max_pages + 1):
            logger.info("Fetching page %d | endpoint=%s", page, endpoint)

            response = self.get(endpoint, params=params, headers=headers)

            if isinstance(response, list):
                items = response
            else:
                items = response.get(results_key, []) if results_key else response

            yield items

            logger.debug(
                "Received %d items (total=%d)",
                len(items),
                len(results),
            )

            cursor = response.get(cursor_key)

            if not cursor:
                logger.info("Cursor pagination finished after %d pages", page)
                break

            params = {**params, cursor_param: cursor}

            time.sleep(sleep)

        else:
            logger.warning("Reached max_pages=%d for %s", max_pages, endpoint)
