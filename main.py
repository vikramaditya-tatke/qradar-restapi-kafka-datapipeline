from typing import Dict
from qradarconnector import QRadarConnector
from settings import settings
from pipeline_logger import logger
from producer import create_producer
from query_builder import construct_base_urls, get_search_params
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timedelta


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error(f"Retry attempt {retry_state.attempt_number} failed."),
    reraise=True,  # Re-raise the final exception
)
def perform_qradar_search(qradar_connector: QRadarConnector, base_url: str, search_params: Dict[str, str]):
    """
    Performs a QRadar search with retry mechanism and detailed logging.
    """
    query_expression = search_params["query"]["query_expression"]
    retry_attempt = 0
    try:
        search = qradar_connector.trigger_search(base_url, query_expression)
        logger.info("Search Triggered")
        cursor_id = search.get("cursor_id")
        if not cursor_id:
            raise ValueError("Invalid search response: missing cursor_id")

        while retry_attempt < settings.max_attempts:
            polling_response_header = qradar_connector.get_search_status(cursor_id)
            logger.info(f"Polling with cursor ID: {cursor_id}")
            if polling_response_header and polling_response_header["completed"]:
                logger.info("Search Completed")
                return polling_response_header
            else:
                logger.debug("Search still in progress...")

    except Exception as e:
        logger.error(f"Error during QRadar search: {e}")
        raise  # Important to re-raise for retry to work


def initialize(base_urls, search_params):
    logger.debug("Initilizing objects")
    producer = create_producer()
    logger.debug("Created Kafka Producer")

    # Hardcoding Console 3 IP for Testing
    qradar_connector = QRadarConnector(
        sec_token=settings.console_3_token,
        base_url=base_urls.get(settings.console_3_ip),
    )
    logger.debug("Created QRadarConnector object")

    polling_response_header = perform_qradar_search(
        qradar_connector,
        base_urls[settings.console_3_ip],
        search_params=search_params,
    )

    return producer, qradar_connector, polling_response_header


if __name__ == "__main__":
    logger.debug("Application Started")
    base_urls = construct_base_urls()
    logger.debug("Constructed base_urls")
    search_params = get_search_params()
    logger.debug("Generated search parameters")
    try:
        producer, qradar_connector, polling_response_header = initialize(
            base_urls,
            search_params,
        )
        if polling_response_header:
            logger.info("Started Data Fetching")
            qradar_connector.receive_data(
                polling_response_header,
                producer,
                polling_response_header["record_count"],
                search_params,
            )
        else:
            logger.warning("Search failed after maximum attempts")

    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
