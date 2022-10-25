import json
import os
import re
from typing import Iterator

import requests


class TowerClient:
    def __init__(self, tower_token=None, tower_api_url=None, debug_mode=False) -> None:
        """Generate NextflowTower instance

        The descriptions below for the user types were copied
        from the Nextflow Tower interface.

        Raises:
            KeyError: The 'NXF_TOWER_TOKEN' environment variable isn't defined
            KeyError: The 'NXF_TOWER_API_URL' environment variable isn't defined
        """
        self.debug = debug_mode
        # Retrieve Nextflow Tower token from environment
        try:
            self.tower_token = (
                tower_token
                or os.environ.get("NXF_TOWER_TOKEN")
                or os.environ.get("TOWER_ACCESS_TOKEN")
            )
        except KeyError as e:
            raise KeyError(
                "The 'NXF_TOWER_TOKEN' environment variable must "
                "be defined with a Nextflow Tower API token."
            ) from e
        # Retrieve Nextflow Tower API URL from environment
        try:
            tower_api_url = (
                tower_api_url
                or os.environ.get("NXF_TOWER_API_URL")
                or os.environ.get("TOWER_API_ENDPOINT")
            )
            assert tower_api_url is not None
        except (KeyError, AssertionError) as e:
            raise KeyError(
                "The 'NXF_TOWER_API_URL' environment variable must "
                "be defined with a Nextflow Tower API URL."
            ) from e
        self.tower_api_base_url = tower_api_url

    def get_valid_name(self, full_name: str) -> str:
        """Generate Tower-friendly name from full name

        Args:
            full_name (str): Full name (with spaces/punctuation)

        Returns:
            str: Name with only alphanumeric, dash and underscore characters
        """
        return re.sub(r"[^A-Za-z0-9_-]", "-", full_name)

    def request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an authenticated HTTP request to the Nextflow Tower API

        Args:
            method (str): An HTTP method (GET, PUT, POST, or DELETE)
            endpoint (str): The API endpoint with the path parameters filled in

        Returns:
            Response: The raw Response object to allow for special handling
        """
        assert method in {"GET", "PUT", "POST", "DELETE"}
        url = self.tower_api_base_url + endpoint
        kwargs["headers"] = {"Authorization": f"Bearer {self.tower_token}"}
        response = requests.request(method, url, **kwargs)
        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = dict()
        if self.debug:
            print(f"\nEndpoint:\t {method} {url}")
            print(f"Params: \t {kwargs.get('params')}")
            print(f"Payload:\t {kwargs.get('json')}")
            print(f"Status Code:\t {response.status_code} / {response.reason}")
            print(f"Response:\t {result}")
        return result

    def paged_request(self, method: str, endpoint: str, **kwargs) -> Iterator[dict]:
        """Iterate through pages of results for a given request

        Args:
            method (str): An HTTP method (GET, PUT, POST, or DELETE)
            endpoint (str): The API endpoint with the path parameters filled in

        Returns:
            Iterator[Dict]: An iterator traversing through pages of responses
        """
        params = kwargs.pop("params", {})
        params["max"] = 50
        num_items = 0
        total_size = 1  # Artificial value for initiating the while-loop
        while num_items < total_size:
            params["offset"] = num_items
            response = self.request(method, endpoint, params=params, **kwargs)
            total_size = response.pop("totalSize", 0)
            _, items = response.popitem()
            for item in items:
                num_items += 1
                yield item
