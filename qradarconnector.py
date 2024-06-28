import requests
from pipeline_logger import logger
import ijson

# TODO: Write a function to trigger the search on QRadar
# TODO: Write a function to check the search status on QRadar


class QRadarConnector:
    def __init__(
        self,
        console_ip: str,
        sec_token: str,
    ) -> requests.Session:
        self.session = requests.Session()

    def parse_qradar_data(self, result):
        """Parses the JSON response line by line, since the response received from QRadar is too large to hold in memory.

        Args:
            result (Response): response object received from the GET request to ariel/searches/{search_id}/results enpoint.

        Yields:
            dict: A single event received from QRadar.
        """
        result.raise_for_status()
        parser = ijson.items(result.raw, "events.item")
        for event in parser:
            yield event

    def receive_data(
        self,
        url,
        total_record_count,
    ):
        """Initaites a GET request to stream the entire search result at once.
        Handles the retry logic for IBM QRadar API failures during data transfer.

        Args:
            url (str): complete URL to send the GET requests to receive the results of the AQL search.
            total_record_count (int): total records found for the search on IBM QRadar Console.

        Yields:
            dict: yeilds the dict yeiled by the parse_qradar_data() method to the Kafka producer.
        """
        current_record_count = 0
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            if current_record_count >= total_record_count:
                break
            headers = self.update_headers(total_record_count, current_record_count)
            try:
                with self.session.get(
                    url,
                    headers=headers,
                    stream=True,
                    verify=False,
                ) as result:
                    for event in self.parse_qradar_data(result):
                        if event:
                            current_record_count += 1
                            yield event
            except (ijson.JSONError, requests.exceptions.RequestException) as e:
                status_code = e.response.status_code
                if status_code >= 400 and status_code < 500:
                    logger.error("Client Error - Check Search Parameters")
                    break
                elif status_code >= 500 and status_code < 600:
                    logger.error(f"Server Error - Retry Attempt {retry_count}")
                    retry_count += 1
            else:
                retry_count = 0  # Reset retry count on succes

    def update_headers_to_restart_failed_searches(
        self,
        total_record_count,
        current_record_count,
    ) -> dict:
        """Updates the Range header to set the starting item to the number of records received before the failure.

        Args:
            total_record_count (int): total records found for the search on IBM QRadar Console.
            current_record_count (int): number of records received before the GET request was interrupted.

        Returns:
            dict: _description_
        """
        return {
            "SEC": self.sec_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "19.0",
            "Range": f"items={current_record_count}-{total_record_count}",
        }

    def trigger_search(self): ...
    def get_search_status(self): ...
