import requests.exceptions
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline_logger import logger
from qradar.query_builder import get_search_params
from settings import settings


# TODO: Correctly handle the situation where a valid search id is not found after multiple polling attempts and start a
# new search.


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error(
        f"Retry attempt {retry_state.attempt_number} failed."
    ),
    reraise=True,  # Re-raise the final exception
)
def search_executor(search_params):
    """
    Performs a QRadar search with retry mechanism and detailed logging.
    Creates a process for each console.
    """
    logger.info(
        f"event_processor: {search_params[0]}, customer_name: {search_params[1]}, query: {search_params[2][0]}"
    )
    qradar_connector = search_params[3]
    search_params, split_query = get_search_params(search_params)
    logger.debug("Generated search parameters")
    query_expression = search_params["query"]["query_expression"]
    retry_attempt = 0
    polling_response_header = None
    try:
        search = qradar_connector.trigger_search(query_expression)
        logger.info("Search Triggered")
        cursor_id = search["cursor_id"]
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
            logger.info(f"Getting QRadar Table Name for {cursor_id}")
            parser_key_response_header = qradar_connector.get_parser_key(
                polling_response_header
            )
            for key, value in parser_key_response_header.items():
                search_params["parser_key"] = key
                search_params["response_header"] = value
            return search_params
        else:
            logger.warning(f"Search failed after maximum attempts for {cursor_id}")
    except (
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.TooManyRedirects,
        requests.exceptions.HTTPError,
    ):
        logger.error("Error occurred while polling for search status")
    except Exception as e:
        logger.exception(f"Error during QRadar search: {e}")
