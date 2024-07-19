import sys
from datetime import datetime

# import ijson.backends.yajl2_cffi as ijson
import ijson
import requests
import ujson
from dateutil.relativedelta import relativedelta, SA
from requests import Session
from requests.exceptions import RequestException
from requests.models import Response
from tqdm import tqdm
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from clickhouseclient import push_data_to_clickhouse
from druid.push_streaming import push_data
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
                method,
                url,
                timeout=self.default_timeout,
                verify=False,
                **kwargs,
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
        params = {
            "query_expression": query_expression,
        }
        response = self._make_request(
            method="POST",
            url=url,
            params=params,
        )
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
        response = self._make_request("GET", url, headers=headers)
        if response:
            return response.json()

    def get_search_data(
        self,
        response_header: dict,
        search_params: dict,
    ):
        """Initiates a GET request to stream the entire search result at once.
        Handles the retry logic for QRadar API failures during data transfer.

        Args:
            response_header (dict) : response header received using the trigger search function.
            search_params (dict) : parameters used for the

        Yields:
            dict: Yields the dict yielded by the _parse_qradar_data() method to the Kafka producer.
        """
        total_record_count = response_header["record_count"]
        current_record_count = 0
        progress_bar = initialize_progress_bar(
            response_header, total_record_count, search_params
        )
        headers = {
            "Range": f"items={current_record_count}-{total_record_count}",
        }
        url = (
            f"{self.base_url}/api/ariel/searches/{response_header['cursor_id']}/results"
        )
        with self.session.get(
            url=url,
            headers=headers,
            stream=True,
            verify=False,
        ) as response:
            response.raw.decode_content = True
            # self.produce_data_batch(
            #     current_record_count, progress_bar, response, search_params
            # )
            self.produce_clickhouse_data_batch(
                current_record_count, progress_bar, response, search_params
            )
        progress_bar.close()

    def produce_clickhouse_data_batch(
        self, current_record_count, progress_bar, response, search_params
    ):
        batch = []
        events = ijson.items(response.raw, "events.item")
        for event in events:
            event = _add_date(event)
            event = _rename_event(event)
            if event:
                current_record_count += 1
                progress_bar.update()
                batch.append(event)
                if len(batch) >= 1000:
                    push_data_to_clickhouse(
                        batch,
                        search_params["customer_name"],
                        search_params["query"]["query_name"],
                    )
                    batch = []
        if len(batch) >= 0:
            push_data_to_clickhouse(
                batch,
                search_params["customer_name"],
                search_params["query"]["query_name"],
            )

    def produce_data_batch(
        self, current_record_count, progress_bar, response, search_params
    ):
        batch = None
        # parser_key = "events"
        # parser_key = _extract_parser_key(response)
        events = ijson.items(response.raw, "events.item")
        for event in events:
            # for event in _parse_qradar_data(response, parser_key):
            event = _add_date(event)
            event = _rename_event(event)
            event = ujson.dumps(event, ensure_ascii=False).encode("utf-8")
            # for event in _parse_qradar_data(response, parser_key):
            # for event in _parse_qradar_data(result):
            if event:
                current_record_count += 1
                # self.producer.produce("demo_topic", event)
                progress_bar.update()

                event_size = sys.getsizeof(event)
                self.current_batch_size += event_size
                batch, batch_full = self.create_payload(event)
                if batch_full:
                    push_data(batch, search_params["query"]["query_name"])
                    self.current_batch = None
                else:
                    self.current_batch_size += event_size
        if batch:
            push_data(batch, search_params["query"]["query_name"])

    def create_payload(self, event_json: bytes):
        """Creates a new-line delimited JSON string batch.

        Args:
            event_json: The event data to add to the batch serialized to json.

        Returns:
            str: The newline-delimited JSON string if the batch size is reached, else None.
            bool: True if the batch size was reached and a payload was returned, else False.
        """
        if self.current_batch is None:
            self.current_batch = b""  # Initialize the batch
        event_size = sys.getsizeof(event_json) + 1  # +1 for the newline character
        if self.current_batch_size + event_size > self.batch_size_limit:
            payload = self.current_batch
            self.current_batch = (
                event_json + b"\n"
            )  # Start a new batch with the current event
            self.current_batch_size = event_size
            return payload, True
        else:
            self.current_batch += event_json + b"\n"
            self.current_batch_size += event_size
            payload = self.current_batch
            return payload, False


# TODO: The dynamic_key required for parsing JSON data should be found automatically.
def _extract_parser_key(response):
    # pattern = re.compile(r"^GV_#NORMAL#\d+$")
    parser = ijson.parse(response.raw)
    for prefix, event, value in parser:
        if event == "start_array":
            array_prefix = f"{prefix}.item"
            return array_prefix


def _parse_qradar_data(response, prefix):
    """Parses the JSON response line by line, since the response received from QRadar is too large to hold in memory.

    Args:
        response (Response): Response object received from the GET request to ariel/searches/{search_id}/results
        endpoint.

    Yields:
        dict: A single event received from QRadar.
    """
    events = ijson.items(
        response.raw, prefix, multiple_values=True, buf_size=1024 * 1024
    )
    for event in events:
        event = _add_date(event)
        event = _rename_event(event)
        yield ujson.dumps(event, ensure_ascii=False).encode("utf-8")


def _rename_event(event):
    mapping = {
        "DomainName(DomainID)": "domainName",
        "domainId": "Domain",
        "DomainAwareFullNetworkName(SourceIP, DomainID)": "Source Network",
        "DomainAwareFullNetworkName(SourceIP)": "Source Network",
        "DateFormatFunction(StartTime, dd/MM/yyyy)": "ReportDate",
        "SensorDeviceName(DeviceId)": "Log Source",
        "QidName(Qid)": "Event Name",
        "destinationIP": "Destination IP",
        "sourceIP": "Source IP",
        "Time": "Start Time",
        "qid": "QID",
        "SUM_eventCount": "Event Count",
        "CategoryName(Category)": "Low Level Category",
        "CategoryName(HighLevelCategory)": "High Level Category",
        "SensorDeviceTypeName(DeviceType)": "Log Source Type",
        "deviceType": "Log Source Type",
        "userName": "Username",
        "username": "Username",
        "magnitude": "Magnitude",
        "Authentication Package": "Authentication Package (custom)",
        "qidEventId": "Event ID",
        "Logon Type": "Logon Type (custom)",
        "Logon ID": "Logon ID (custom)",
        "Impersonation Level": "Impersonation Level (custom)",
        "Source Workstation": "Source Workstation (custom)",
        "Process Name": "Process Name (custom)",
        "destinationGeographicLocation": "Destination Geographic Country/Region",
        "sourceGeographicLocation": "Source Geographic Country/Region",
        "destinationPort": "Destination Port",
        "Source Port": "Source Port",
    }
    renamed_event = {mapping.get(k, k): v for k, v in event.items()}
    return renamed_event


def _add_date(line_json):
    """
    Enhances a JSON object with date-related fields:

    - WeekFrom: The previous Saturday's date.
    - ReportDate: The date extracted from the JSON, formatted.
    - createdAt: The current UTC timestamp.

    Args: line_json: A dictionary-like JSON object containing either "Start Time" or "Time" (in milliseconds or
    seconds since the epoch).

    Returns:
        The modified JSON object.
    """

    query_date_epoch = line_json.get("Start Time") or line_json.get("Time")

    if query_date_epoch is None:
        raise ValueError("Missing 'Start Time' or 'Time' key in JSON data.")

    # Determine timestamp type (milliseconds or seconds) and adjust if needed
    query_timestamp = (
        query_date_epoch / 1000 if query_date_epoch > 1e10 else query_date_epoch
    )

    base_date = datetime.fromtimestamp(query_timestamp)
    previous_saturday = base_date + relativedelta(weekday=SA(-1))

    line_json["WeekFrom"] = previous_saturday.strftime("%d/%m/%Y")
    line_json["ReportDate"] = base_date.strftime("%d/%m/%Y")

    return line_json


def initialize_progress_bar(response_header, total_record_count, search_params):
    return tqdm(
        total=total_record_count,
        desc=f"Receiving data for {search_params['customer_name']} {response_header['cursor_id']}",
    )
