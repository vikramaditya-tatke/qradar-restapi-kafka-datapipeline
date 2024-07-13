from requests import Session
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import get_search_params, construct_base_urls
from settings import settings


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error(f"Retry attempt {retry_state.attempt_number} failed."),
    reraise=True,  # Re-raise the final exception
)
def search_executor(search_params):
    """
    Performs a QRadar search with retry mechanism and detailed logging.
    Creates a process for each console.
    """
    logger.info(f"event_processor: {search_params[0]}, customer_name: {search_params[1]}, query: {search_params[2][0]}")
    search_params, split_query = get_search_params(search_params)
    logger.debug("Generated search parameters")
    session = Session()
    qradar_connector = QRadarConnector(
        sec_token=settings.console_3_token,
        session=session,
        base_url=construct_base_urls(),
    )
    query_expression = search_params["query"]["query_expression"]
    retry_attempt = 0
    polling_response_header = None
    try:
        search = qradar_connector.trigger_search(query_expression)
        logger.info("Search Triggered")
        cursor_id = search.get("cursor_id")
        if not cursor_id:
            raise ValueError("Invalid search response: missing cursor_id")
        while retry_attempt < settings.max_attempts:
            retry_attempt += 1
            polling_response_header = qradar_connector.get_search_status(cursor_id)
            logger.info(f"Polling with cursor ID: {cursor_id}")
            if polling_response_header and polling_response_header["completed"]:
                logger.info("Search Completed")
                break
            else:
                logger.debug("Search still in progress...")

        if polling_response_header:
            logger.info(f"Started Data Fetching for {cursor_id}")
            qradar_connector.get_search_data(polling_response_header, search_params)
        else:
            logger.warning(f"Search failed after maximum attempts for {cursor_id}")

    except Exception as e:
        logger.error(f"Error during QRadar search: {e}")
        raise  # Important to re-raise for retry to work
