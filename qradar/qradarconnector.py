from typing import Generator, Dict, Any

import ijson
import requests
from requests import Session, Response
from requests.exceptions import HTTPError, ReadTimeout
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from settings import settings

# Disable warnings about insecure HTTPS requests (if using self-signed certs, for example)
disable_warnings(InsecureRequestWarning)


class QRadarConnector:
    def __init__(self, sec_token: str, session: Session, base_url: str):
        self.session = session
        self.session.headers = {
            "SEC": sec_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "22.0",
        }
        self.sec_token = sec_token
        self.base_url = base_url
        self.default_timeout = settings.default_timeout
        self.max_search_ttc_in_seconds = settings.max_search_ttc_in_seconds
        self.current_record_count = 0

    def _make_request(self, method: str, url: str, **kwargs) -> Response:
        """Makes a request to the QRadar API with error handling.

        Args:
            method (str): The HTTP method (GET, POST).
            url (str): The URL to request.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            requests.Response: The response object.

        Raises:
            RequestException: For request-related errors.
        """
        try:
            response = self.session.request(
                method=method,
                url=url,
                timeout=self.default_timeout,
                verify=False,
                **kwargs,
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except HTTPError:
            # Log specific HTTP errors
            raise
        except ReadTimeout:
            raise
        except Exception:
            raise

    def trigger_search(self, query_expression: dict) -> dict:
        """Triggers a QRadar search using the provided query expression.

        Args:
            query_expression (dict): The AQL query to send to QRadar.

        Returns:
            dict: Response Header
        """
        url = f"{self.base_url}/api/ariel/searches"

        try:
            response = self._make_request(
                method="POST", url=url, params={"query_expression": query_expression}
            )
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            raise

    def get_search_status(self, cursor_id: str) -> dict:
        """Gets the status of a QRadar search using the cursor ID.

        Args:
            cursor_id (str): The cursor ID for the search.

        Returns:
            dict: The status of the search.
        """
        url = f"{self.base_url}/api/ariel/searches/{cursor_id}"
        headers = {
            "Prefer": f"wait={self.max_search_ttc_in_seconds}",
        }
        response = self._make_request("GET", url, headers=headers)
        return response.json()

    def _extract_parser_key(self, url: str) -> str | None:
        """Finds the dynamic key required for parsing JSON data.

        Args:
            url (str): The URL to request for extracting the parser key.

        Returns:
            str: The dynamic key for parsing the JSON data.
        """
        parser_key = None
        with self.session.get(url=url, stream=True, verify=False) as response:
            parser = ijson.parse(response.raw)
            for prefix, event, value in parser:
                if event == "start_array":
                    parser_key = f"{prefix}.item"
                    break
        return parser_key

    def get_parser_key(self, response_header: dict) -> dict | None:
        """Initiates a GET request to stream the entire search result at once.

        Args:
            response_header (dict): Response header received using the trigger search function.

        Returns:
            dict: A dictionary containing the parser key and the response header.
        """
        url = (
            f"{self.base_url}/api/ariel/searches/{response_header['cursor_id']}/results"
        )
        parser_key = self._extract_parser_key(url)
        return {parser_key: response_header} if parser_key else None

    def fetch_data(self, cursor_id: str, max_record_count: int) -> requests.Response:
        current_record_count = 0
        response = self.session.get(
            url=f"{self.base_url}/api/ariel/searches/{cursor_id}/results",
            headers={"Range": f"items={current_record_count}-{max_record_count}"},
            stream=True,
            verify=False,
        )
        response.raise_for_status()
        return response


def parse_qradar_data(
    response: requests.Response, parser_key: str
) -> Generator[Dict[str, Any], None, None]:
    try:
        for event in ijson.items(response.raw, parser_key):
            yield event
    except ValueError:
        raise
    except ijson.common.IncompleteJSONError:
        raise
