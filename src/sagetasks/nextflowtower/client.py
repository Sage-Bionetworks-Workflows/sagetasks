import json
import os
import re
from typing import Iterator

import requests


class TowerClient:
    def __init__(
        self,
        tower_token: str = None,
        tower_api_url: str = None,
        debug_mode: bool = False,
    ) -> None:
        """Simple Python client for making requests to Nextflow Tower.

        Args:
            tower_token (str): Tower (bearer) access token for authentication.
                https://help.tower.nf/22.3/api/overview/#openapi
            tower_api_url (str): Base URL for the Tower API.
            debug_mode (bool): Whether to log HTTP requests.

        Raises:
            KeyError: The 'NXF_TOWER_TOKEN' environment variable isn't defined
            KeyError: The 'NXF_TOWER_API_URL' environment variable isn't defined
        """
        # Initialize instance attributes
        self.tower_token = (
            tower_token
            or os.environ.get("NXF_TOWER_TOKEN")
            or os.environ.get("TOWER_ACCESS_TOKEN")  # Backwards-compatible
        )
        self.tower_api_base_url = (
            tower_api_url
            or os.environ.get("NXF_TOWER_API_URL")
            or os.environ.get("TOWER_API_ENDPOINT")  # Backwards-compatible
        )
        self.debug = debug_mode
        # Check for empty values
        if self.tower_token is None:
            raise ValueError(
                "Provide a Nextflow Tower access token using the `tower_token` "
                "argument or the `NXF_TOWER_TOKEN` environment variable."
            )
        if self.tower_api_base_url is None:
            raise ValueError(
                "Provide the Nextflow Tower API base URL using the `tower_api_url` "
                "argument or the `NXF_TOWER_API_URL` environment variable."
            )

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
            **kwargs: Additional named arguments passed through to
                requests.request().

        Returns:
            Response: The raw Response object to allow for special handling
        """
        valid_methods = {"GET", "PUT", "POST", "DELETE"}
        if method not in valid_methods:
            raise ValueError(
                f"Specified method ({method}) isn't a valid option ({valid_methods})."
            )
        url = self.tower_api_base_url + endpoint
        kwargs["headers"] = {"Authorization": f"Bearer {self.tower_token}"}
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = dict()
        if self.debug:
            # TODO: Switch from print to logging
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
            **kwargs: Additional named arguments passed through to
                requests.request().

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
