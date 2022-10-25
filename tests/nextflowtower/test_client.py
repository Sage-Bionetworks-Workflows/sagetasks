import os

import pytest

from sagetasks.nextflowtower import client

TEST_TOKEN = "token"
TEST_API_URL = "https://example.com"


@pytest.fixture
def tower_client():
    return client.TowerClient(TEST_TOKEN, TEST_API_URL)


def test_import():
    assert client


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
        with pytest.raises(KeyError):
            client.TowerClient()

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

    def test_request(self, tower_client):
        # TODO: Check for token
        # TODO: Check for base URL
        # TODO: Check for method
        # TODO: Check for non-empty response
        # TODO: Check for empty response
        # TODO: Check for debugging output
        assert tower_client

    def test_paged_request(self, tower_client):
        # TODO: Check for proper request format
        # TODO: Check for generator return value
        # TODO: Check for follow-up request
        assert tower_client
