from unittest.mock import MagicMock, patch

import pytest

from gamelens.clients.steam_client import SteamAPI
from gamelens.models.steam import SteamAppListItem


@pytest.fixture
def steam_client():
    return SteamAPI(api_key="dummy_key")


def test_get_app_list_single_page(steam_client):
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
        assert all(isinstance(app, SteamAppListItem) for app in apps)
        mock_request.assert_called_once()


def test_get_app_list_pagination(steam_client):
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
        assert {app.appid for app in apps} == {1, 2}
        assert mock_request.call_count == 2


def test_request_retries_on_429(steam_client):
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


def test_request_fails_after_max_retries(steam_client):
    """Should raise RuntimeError after max retries fail."""
    mock_session = MagicMock()
    mock_session.get.side_effect = Exception("Network error")
    steam_client.session = mock_session

    with pytest.raises(RuntimeError):
        steam_client._request("http://dummy.url", {})
