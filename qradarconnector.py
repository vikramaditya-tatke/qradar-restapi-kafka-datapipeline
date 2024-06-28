import requests
from pipeline_logger import logger
import ijson
from confluent_kafka import Producer


class QRadarConnector:
    def __init__(
        self,
        sec_token: str,
        base_url: str,
    ) -> requests.Session:
        self.session = requests.Session()
        self.session.headers = {
            "SEC": sec_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "20.0",
        }
        self.default_timeout = 120
        self.base_url = base_url

    def trigger_search(
        self,
        url: str,
        params: dict,
    ) -> dict:
        """Takes the api endpoint to POST the search query

        Args:
            url (str): api endpoint to POST the search query. Most likely /api/ariel/searches.
            params (dict): params dictionary containing the query expression.

        Returns:
            dict: reponse received from QRadar.
        """
        try:
            result = self.session.post(
                url=url,
                params=params,
                verify=False,
                timeout=self.default_timeout,
            )
            result.raise_for_status()
            return result.json()
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            if status_code >= 400 and status_code < 500:
                logger.error("Client Error - Check Search Parameters")
            elif status_code >= 500 and status_code < 600:
                logger.error(f"Server Error")

    def get_search_status(
        self,
        cursor_id,
    ) -> dict:
        f"""Polls QRadar using the cursor_id and by defaults waits for {self.max_search_ttc_in_seconds}

        Args:
            url (str): /api/ariel/searches/search_id

        Returns:
            dict: Response received from the polling endpoint
        """
        try:
            result = self.session.get(
                url=f"{self.base_url}api/ariel/searches/{cursor_id}",
                headers={
                    "Prefer": f"wait={self.max_search_ttc_in_seconds}",
                },
                verify=False,
                timeout=self.timeout,
            )
            result.raise_for_status()
            return result.json()
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            if status_code >= 400 and status_code < 500:
                logger.error("Client Error - Check Search Parameters")
            elif status_code >= 500 and status_code < 600:
                logger.error(f"Server Error")

    def receive_data(
        self,
        response_header: dict,
        total_record_count: int,
        producer: Producer,
    ):
        """Initaites a GET request to stream the entire search result at once.
        Handles the retry logic for QRadar API failures during data transfer.

        Args:
            url (str): Complete URL to send the GET requests to receive the results of the AQL search.
            total_record_count (int): total records found for the search on QRadar Console.

        Yields:
            dict: Yeilds the dict yeiled by the parse_qradar_data() method to the Kafka producer.
        """
        current_record_count = 0
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            if current_record_count >= total_record_count:
                break
            headers = {
                "Range": f"items={current_record_count}-{total_record_count}",
            }
            try:
                with self.session.get(
                    url=f"{self.base_url}api/ariel/searches/{response_header['cursor_id']}/results",
                    headers=headers,
                    stream=True,
                    verify=False,
                ) as result:
                    for event in parse_qradar_data(result):
                        if event:
                            current_record_count += 1
                            producer.produce("demo_topic", event)
            except requests.exceptions.RequestException as http_err:
                logger.error(
                    f"Server Error - Retrying from record number {current_record_count}"
                )
                retry_count += 1
            else:
                retry_count = 0  # Reset retry count on succes


def parse_qradar_data(result):
    """Parses the JSON response line by line, since the response received from QRadar is too large to hold in memory.

    Args:
        result (Response): Response object received from the GET request to ariel/searches/{search_id}/results enpoint.

    Yields:
        dict: A single event received from QRadar.
    """
    result.raise_for_status()
    parser = ijson.items(result.raw, "events.item")
    for event in parser:
        yield event
