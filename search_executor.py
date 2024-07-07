from typing import Dict
from tenacity import retry, stop_after_attempt, wait_exponential
from settings import settings
from pipeline_logger import logger
from qradarconnector import QRadarConnector


# TODO: Add the ability to trigger searches on all consoles


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error(f"Retry attempt {retry_state.attempt_number} failed."),
    reraise=True,  # Re-raise the final exception
)
def perform_qradar_searches(
    qradar_connector: QRadarConnector,
    search_params: Dict[str, str],
):
    """
    Performs a QRadar search with retry mechanism and detailed logging.
    Creates a process for each console.
    """
    query_expression = search_params["query"]["query_expression"]
    retry_attempt = 0
    try:
        search = qradar_connector.trigger_search(query_expression)
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
