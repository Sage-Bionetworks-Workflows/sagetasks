import json
import os
from types import GeneratorType

import pytest

from sagetasks.nextflowtower import client

EG_TOKEN = "token"
EG_API_URL = "https://example.com"
EG_METHOD = "GET"
EG_ENDPOINT = "/example/endpoint"


@pytest.fixture
def tower_client():
    return client.TowerClient(EG_TOKEN, EG_API_URL)


class TestTowerClient:
    def test_missing_args(self):
        # Clear environment
        env_vars = [
            "NXF_TOWER_TOKEN",
            "TOWER_ACCESS_TOKEN",
            "NXF_TOWER_API_URL",
            "TOWER_API_ENDPOINT",
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]
        # Ensure exception when initializing
        with pytest.raises(ValueError):
            client.TowerClient()
        with pytest.raises(ValueError):
            client.TowerClient(tower_token=EG_TOKEN)
        with pytest.raises(ValueError):
            client.TowerClient(tower_api_url=EG_API_URL)

    def test_get_valid_name(self, tower_client):
        expectations = {
            "foobar": "foobar",
            "FooBar": "FooBar",
            "foo-bar": "foo-bar",
            "foo_bar": "foo_bar",
            "f00b4r": "f00b4r",
            "foo,bar": "foo-bar",
            "foo (bar)": "foo--bar-",
        }
        for name, output in expectations.items():
            assert tower_client.get_valid_name(name) == output

    def test_request_nonmethod(self, tower_client):
        with pytest.raises(ValueError):
            tower_client.request("FOO", EG_ENDPOINT)

    def test_request_nonempty(self, mocker, capfd, tower_client):
        # Setup
        eg_kwargs = {"foo": "bar"}
        mocked_request = mocker.patch("requests.request", autospec=True)
        mocked_request.return_value.json.return_value = eg_kwargs

        # Regular call with non-empty response (w/o debugging)
        result = tower_client.request(EG_METHOD, EG_ENDPOINT, params=eg_kwargs)
        (method, url), kwargs = mocked_request.call_args
        captured = capfd.readouterr()
        mocked_request.assert_called_once()
        assert method == EG_METHOD
        assert url.startswith(EG_API_URL)
        assert url.endswith(EG_ENDPOINT)
        assert "Authorization" in kwargs["headers"]
        assert kwargs["headers"]["Authorization"] == f"Bearer {EG_TOKEN}"
        assert "params" in kwargs
        assert kwargs["params"] == eg_kwargs
        assert result == eg_kwargs
        assert captured.out == ""

    def test_request_empty(self, mocker, capfd, tower_client):
        # Setup
        mocked_request = mocker.patch("requests.request", autospec=True)

        def raise_json_error():
            raise json.decoder.JSONDecodeError("", "", 0)

        # Regular call with empty response (w/ debugging)
        mocked_request.return_value.json.side_effect = raise_json_error
        tower_client.debug = True
        result = tower_client.request(EG_METHOD, EG_ENDPOINT)
        captured = capfd.readouterr()
        assert result == dict()
        assert captured.out != ""
        assert "Endpoint:" in captured.out
        assert "Params:" in captured.out
        assert "Payload:" in captured.out
        assert "Status Code:" in captured.out
        assert "Response:" in captured.out

    def test_paged_request(self, mocker, tower_client):
        # Setup
        eg_responses = [
            {"things": ["foo"], "totalSize": 2},
            {"things": ["bar"], "totalSize": 2},
        ]
        mocked_request = mocker.patch.object(tower_client, "request", autospec=True)
        mocked_request.side_effect = eg_responses

        # Regular call with non-empty response
        raw_result = tower_client.paged_request(EG_METHOD, EG_ENDPOINT)
        result = list(raw_result)  # Forcing generator to iterate
        (method, endpoint), kwargs = mocked_request.call_args
        assert isinstance(raw_result, GeneratorType)
        mocked_request.assert_called()
        assert mocked_request.call_count == 2
        assert method == EG_METHOD
        assert endpoint == EG_ENDPOINT
        assert "params" in kwargs
        assert "max" in kwargs["params"]
        assert "offset" in kwargs["params"]
        assert result == ["foo", "bar"]
