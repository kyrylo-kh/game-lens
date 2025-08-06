from unittest.mock import MagicMock, patch

import pytest

from gamelens.clients.steam_client import SteamAPI


@pytest.fixture
def steam_client():
    return SteamAPI(api_key="dummy_key", backoff_factor=0.1, max_retries=3, timeout=5)


class TestGetAppList:
    def test_single_page(self, steam_client):
        """Should return apps from a single-page response."""
        mock_response = {
            "response": {
                "apps": [
                    {"appid": 1, "name": "Game1", "last_modified": 123, "price_change_number": 1},
                    {"appid": 2, "name": "Game2", "last_modified": 124, "price_change_number": 2},
                ],
                "have_more_results": False,
            }
        }
        with patch.object(steam_client, "_request", return_value=mock_response) as mock_request:
            apps = steam_client.get_app_list()
            assert len(apps) == 2
            assert all(isinstance(app, dict) for app in apps)
            mock_request.assert_called_once()

    def test_pagination(self, steam_client):
        """Should handle multi-page responses and concatenate results."""
        page1 = {
            "response": {
                "apps": [{"appid": 1, "name": "Game1"}],
                "have_more_results": True,
                "last_appid": 1,
            }
        }
        page2 = {
            "response": {
                "apps": [{"appid": 2, "name": "Game2"}],
                "have_more_results": False,
            }
        }
        with patch.object(steam_client, "_request", side_effect=[page1, page2]) as mock_request:
            apps = steam_client.get_app_list()
            assert len(apps) == 2
            assert {app.get("appid") for app in apps} == {1, 2}
            assert mock_request.call_count == 2


class TestRequest:
    def test_retries_on_429(self, steam_client):
        """Should retry on 429 and succeed after backoff."""
        good_response = {"response": {"apps": []}}
        mock_session = MagicMock()
        mock_session.get.side_effect = [
            MagicMock(status_code=429, json=lambda: {}),
            MagicMock(status_code=429, json=lambda: {}),
            MagicMock(status_code=200, json=lambda: good_response),
        ]
        steam_client.session = mock_session
        with patch("time.sleep", return_value=None) as sleep_mock:
            result = steam_client._request("http://dummy.url", {})
            assert result == good_response
            assert mock_session.get.call_count == 3
            assert sleep_mock.call_count == 2

    def test_fails_after_max_retries(self, steam_client):
        """Should raise RuntimeError after max retries fail."""
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("Network error")
        steam_client.session = mock_session
        with pytest.raises(RuntimeError):
            steam_client._request("http://dummy.url", {})


class TestGetAppDetails:
    def test_get_app_details_success(self, steam_client):
        """Should return raw app details when API responds with success."""
        mock_response = {
            "10": {
                "success": True,
                "data": {"steam_appid": 10, "name": "Counter-Strike", "type": "game", "is_free": False},
            }
        }
        with patch.object(steam_client, "_request", return_value=mock_response) as mock_request:
            details = steam_client.get_app_details(10)
            assert details["success"] is True
            assert details["data"]["steam_appid"] == 10
            mock_request.assert_called_once_with(f"{steam_client.STORE_URL}/appdetails", {"appids": 10})

    def test_get_app_details_failure(self, steam_client):
        """Should return empty dict if API response missing or failed."""
        mock_response = {"11": {"success": False}}
        with patch.object(steam_client, "_request", return_value=mock_response):
            details = steam_client.get_app_details(999999)
            assert details == {}
