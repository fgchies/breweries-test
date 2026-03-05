from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


@dataclass(frozen=True)
class OpenBreweryClient:
    base_url: str = "https://api.openbrewerydb.org/v1/breweries"
    timeout_seconds: int = 10
    max_retries: int = 5
    backoff_multiplier: float = 0.5

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = requests.get(self.base_url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected API response format")

        return data

    def iter_breweries(
        self,
        per_page: int = 200,
        start_page: int = 1,
        max_pages: Optional[int] = None,
        **filters: Any,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Iterates through brewery pages until the API returns an empty list."""
        page = start_page

        while True:
            if max_pages is not None and page > max_pages:
                return

            params = {"page": page, "per_page": per_page, **filters}
            breweries = self._request(params)

            if not breweries:
                return

            yield breweries
            page += 1