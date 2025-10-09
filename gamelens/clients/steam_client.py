import logging
import time
from typing import Any, Dict, List, Optional

import requests
from requests import Response

from gamelens.settings import settings

logger = logging.getLogger(__name__)


class SteamAPI:
    """
    Client for interacting with the Steam Web API.
    Provides methods to fetch game lists, details, and other resources.

    Args:
        api_key: Steam API key (if None, uses settings.steam_api_key).
        max_retries: Number of retry attempts for failed requests.
        backoff_factor: Delay multiplier for retries (exponential backoff).
        timeout: HTTP request timeout in seconds.
        session: Optional pre-configured requests.Session for connection reuse.
    """

    BASE_URL = "https://api.steampowered.com"
    STORE_URL = "https://store.steampowered.com/api"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.5,
        timeout: int = 10,
        session: Optional[requests.Session] = None,
        requests_delay: float = 0.5,
    ):
        self.api_key = api_key or settings.steam_api_key
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.session = session or requests.Session()
        self.requests_delay = requests_delay

        self.last_request_time = 0

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting delay before making a request."""
        if self.requests_delay <= 0:
            return

        elapsed = time.time() - self.last_request_time
        if elapsed < self.requests_delay:
            time.sleep(self.requests_delay)

    def _request(self, url: str, params: Dict[str, Any]) -> dict:
        """
        Perform a GET request with retry and exponential backoff.

        Args:
            url: Full API endpoint URL.
            params: Query parameters for the request.

        Returns:
            Parsed JSON response as a Python dictionary.

        Raises:
            RuntimeError: If all retry attempts fail.
            requests.RequestException: If a non-recoverable HTTP error occurs.
        """
        self._apply_rate_limit()

        for attempt in range(1, self.max_retries + 1):
            try:
                self.last_request_time = time.time()
                resp: Response = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 429:  # Rate limited
                    wait = self.backoff_factor * attempt
                    logger.warning(f"Rate limited on {url}. Waiting {wait:.1f}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.error(f"Request to {url} failed (attempt {attempt}): {e}")
                time.sleep(self.backoff_factor * attempt)
        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")

    def get_app_list(self, max_results: int = 50000) -> List[Dict[str, Any]]:
        """
        Fetch the full list of Steam apps (games only), handling pagination.

        Args:
            max_results: Maximum number of apps to fetch per request (default: 50,000).

        Returns:
            List[Dict]: List of app dictionaries containing appid, name, and other details.
        """
        url = f"{self.BASE_URL}/IStoreService/GetAppList/v1/"
        last_appid = 0
        all_apps: List[Dict[str, Any]] = []

        while True:
            params = {
                "key": self.api_key,
                "include_games": True,
                "include_dlc": False,
                "include_software": False,
                "include_videos": False,
                "include_hardware": False,
                "max_results": max_results,
                "last_appid": last_appid,
            }
            data = self._request(url, params)
            response = data.get("response", {})
            apps = response.get("apps", [])
            all_apps.extend(apps)

            if not response.get("have_more_results"):
                break

            last_appid = response.get("last_appid")
            logger.info(f"Fetched {len(all_apps)} apps so far. Continuing from appid={last_appid}")

        logger.info(f"Fetched total {len(all_apps)} apps from Steam.")
        return all_apps


    def get_app_details(self, appid: int) -> Dict[str, Any]:
        """
        Fetch raw details for a single app.

        Args:
            appid: Steam application ID.

        Returns:
            Dict: Raw Steam AppDetails response.
        """
        url = f"{self.STORE_URL}/appdetails"
        params = {"appids": appid}
        data = self._request(url, params)
        return data.get(str(appid), {})
