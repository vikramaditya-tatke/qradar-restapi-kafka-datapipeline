import ijson
import requests
from requests import Session
from requests.exceptions import RequestException
from requests.models import Response
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from settings import settings

disable_warnings(InsecureRequestWarning)


class QRadarSearchError(RequestException):
    """Base exception class for QRadar search errors."""

    def __init__(self, message: str, response: Response = None):
        """Initializes the QRadarSearchError with a message and an optional response object."""
        super().__init__(message)
        self.response = response


class QRadarClientError(QRadarSearchError):
    """Exception raised for 4xx client errors from QRadar."""

    def __init__(self, message: str, response: Response):
        """Initializes the QRadarClientError with the response details."""
        super().__init__(message, response)
        self.status_code = response.status_code
        self.error_message = response.json().get("message", "Unknown error")

    def __str__(self):
        """Returns a string representation of the exception with relevant details."""
        return f"QRadar Client Error {self.status_code}: {self.error_message} - {self.response.url}"


class QRadarServerError(QRadarSearchError):
    """Exception raised for 5xx server errors from QRadar."""

    def __init__(self, message: str, response: Response):
        """Initializes the QRadarServerError with the response details."""
        super().__init__(message, response)
        self.status_code = response.status_code
        self.error_message = response.text  # Server errors might not return JSON

    def __str__(self):
        """Returns a string representation of the exception with relevant details."""
        return f"QRadar Server Error {self.status_code}: {self.error_message} - {self.response.url}"


class QRadarConnector:

    def __init__(self, sec_token: str, session: Session, base_url: str):
        self.current_record_count = None
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
        self.current_batch_size = 0
        self.current_batch = None

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Makes a request to the QRadar API with retry logic.

        Args:
            method (str): The HTTP method (GET, POST).
            url (str): The URL to request.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            requests.Response: The response object.

        Raises:
            QRadarClientError: If a 4xx error occurs.
            QRadarServerError: If a 5xx error occurs.
            requests.RequestException: For other request errors.
        """
        try:
            response = self.session.request(
                method, url, timeout=self.default_timeout, verify=False, **kwargs
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.HTTPError as http_err:
            if 400 <= http_err.response.status_code < 500:
                raise QRadarClientError(
                    f"Client Error", http_err.response
                ) from http_err
            elif 500 <= http_err.response.status_code < 600:
                raise QRadarServerError(
                    f"Server Error", http_err.response
                ) from http_err
            else:
                raise
        except requests.exceptions.ReadTimeout as _read_timeout:
            raise requests.exceptions.ReadTimeout
        except Exception:
            raise

    def trigger_search(self, query_expression: dict) -> dict:
        """Triggers a QRadar search using the provided query expression.

        Args:
            query_expression (str): the AQL query to send to QRadar.

        Returns:
            dict: Response Header
        """

        url = f"{self.base_url}/api/ariel/searches"
        params = {"query_expression": query_expression}
        response = self._make_request(method="POST", url=url, params=params)
        if response:
            return response.json()

    def get_search_status(self, cursor_id: str) -> dict:
        """Gets the status of a QRadar search using the cursor ID."""

        url = f"{self.base_url}/api/ariel/searches/{cursor_id}"
        headers = {
            "Prefer": f"wait={self.max_search_ttc_in_seconds}",
            "SEC": self.sec_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "20.0",
        }
        response = self._make_request(
            "GET",
            url,
            headers=headers,
        )
        if response:
            return response.json()

    # TODO: The dynamic_key required for parsing JSON data should be found automatically.
    def _extract_parser_key(self, url):
        array_prefix = None
        response = self.session.get(url=url, stream=True, verify=False)
        parser = ijson.parse(response.raw)
        for prefix, event, value in parser:
            if event == "start_array":
                array_prefix = f"{prefix}.item"
                break
        response.close()
        return array_prefix

    def get_parser_key(self, response_header: dict):
        """Initiates a GET request to stream the entire search result at once.
        Handles the retry logic for QRadar API failures during data transfer.

        Args:
            response_header (dict) : response header received using the trigger search function.

        Yields:
            dict: Yields the dict yielded by the _parse_qradar_data() method to the Kafka producer.
        """
        self.current_record_count = 0
        url = (
            f"{self.base_url}/api/ariel/searches/{response_header['cursor_id']}/results"
        )
        parser_key = self._extract_parser_key(url)
        return {parser_key: response_header}
