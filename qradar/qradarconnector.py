import ijson
import requests
from requests import Session, Response
from requests.exceptions import RequestException, HTTPError, ReadTimeout
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
            "Version": "20.0",
        }
        self.sec_token = sec_token
        self.base_url = base_url
        self.default_timeout = settings.default_timeout
        self.max_search_ttc_in_seconds = settings.max_search_ttc_in_seconds
        self.batch_size_limit = settings.batch_size_limit * 1023  # Just under 1MB
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
                method, url, timeout=self.default_timeout, verify=False, **kwargs
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except HTTPError as http_err:
            # Log specific HTTP errors
            if 400 <= http_err.response.status_code < 500:
                raise RequestException(
                    f"Client Error {http_err.response.status_code}: {http_err.response.text}"
                )
            elif 500 <= http_err.response.status_code < 600:
                raise RequestException(
                    f"Server Error {http_err.response.status_code}: {http_err.response.text}"
                )
            else:
                raise
        except ReadTimeout:
            raise RequestException("Request timed out.")
        except Exception as err:
            raise RequestException(f"An unexpected error occurred: {err}")

    def trigger_search(self, query_expression: dict) -> dict:
        """Triggers a QRadar search using the provided query expression.

        Args:
            query_expression (dict): The AQL query to send to QRadar.

        Returns:
            dict: Response Header
        """
        url = f"{self.base_url}/api/ariel/searches"
        response = self._make_request(
            method="POST", url=url, params={"query_expression": query_expression}
        )
        return response.json()

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

    def _extract_parser_key(self, url: str) -> str:
        """Finds the dynamic key required for parsing JSON data.

        Args:
            url (str): The URL to request for extracting the parser key.

        Returns:
            str: The dynamic key for parsing the JSON data.
        """
        with self.session.get(url=url, stream=True, verify=False) as response:
            parser = ijson.parse(response.raw)
            for prefix, event, value in parser:
                if event == "start_array":
                    return f"{prefix}.item"
        return None

    def get_parser_key(self, response_header: dict) -> dict:
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
