from gzip import compress

import requests
from requests import Session
from requests.exceptions import RequestException
from requests.models import Response
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from settings import settings

disable_warnings(InsecureRequestWarning)

session = Session()
session.headers = {
    "Authorization": f"Basic {settings.imply_api_key}",
    "Content-Type": "application/json",
    "Content-Encoding": "gzip",
}


class ImplySearchError(RequestException):
    """Base exception class for Imply search errors."""

    def __init__(self, message: str, response: Response = None):
        """Initializes the ImplySearchError with a message and an optional response object."""
        super().__init__(message)
        self.response = response


class ImplyClientError(ImplySearchError):
    """Exception raised for 4xx client errors from Imply."""

    def __init__(self, message: str, response: Response):
        """Initializes the ImplyClientError with the response details."""
        super().__init__(message, response)
        self.status_code = response.status_code
        self.error_message = response.json().get("message", "Unknown error")

    def __str__(self):
        """Returns a string representation of the exception with relevant details."""
        return f"Imply Client Error {self.status_code}: {self.error_message} - {self.response.url}"


class ImplyServerError(ImplySearchError):
    """Exception raised for 5xx server errors from Imply."""

    def __init__(self, message: str, response: Response):
        """Initializes the ImplyServerError with the response details."""
        super().__init__(message, response)
        self.status_code = response.status_code
        self.error_message = response.text  # Server errors might not return JSON

    def __str__(self):
        """Returns a string representation of the exception with relevant details."""
        return f"Imply Server Error {self.status_code}: {self.error_message} - {self.response.url}"


def push_data(payload_bytes: bytes, push_streaming_endpoint: str):
    payload = compress(data=payload_bytes, compresslevel=9)
    push_url = f"{settings.imply_base_url}/{settings.imply_project_id}/events/{push_streaming_endpoint}"
    try:
        response = session.post(
            url=push_url,
            data=payload,
            headers={
                "Authorization": f"Basic {settings.imply_api_key}",
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        if 400 <= http_err.response.status_code < 500:
            raise ImplyClientError(f"Client Error", http_err.response) from http_err

        elif 500 <= http_err.response.status_code < 600:
            raise ImplyServerError(f"Server Error", http_err.response) from http_err
        else:
            raise
    except Exception as e:
        print(e)
