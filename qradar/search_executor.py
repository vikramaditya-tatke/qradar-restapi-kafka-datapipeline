import requests.exceptions
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline_logger import logger
from qradar.query_builder import get_search_params
from settings import settings


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
    event_processor, customer_name, query, qradar_connector = search_params
    search_response = {}
    try:
        search_params, split_query = get_search_params(
            (event_processor, customer_name, query, qradar_connector)
        )
        logger.debug(
            "Generated search parameters",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": search_response,
            },
        )

        search_response = qradar_connector.trigger_search(
            search_params["query"]["query_expression"]
        )
        logger.info(
            "Search Triggered",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": search_response,
            },
        )

        cursor_id = search_response["cursor_id"]
        search_params["attempt"] = 0

        while search_params["attempt"] < settings.max_attempts:
            search_params["attempt"] += 1
            polling_response_header = qradar_connector.get_search_status(cursor_id)
            logger.info(
                "Search Status Polling",
                extra={
                    "ApplicationLog": search_params,
                    "QRadarLog": search_response,
                },
            )

            if polling_response_header and polling_response_header["completed"]:
                logger.info(
                    "Search Completed",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": search_response,
                    },
                )
                return _handle_successful_search(
                    polling_response_header, search_params, qradar_connector
                )

            logger.info(
                "Search Running",
                extra={"ApplicationLog": search_params, "QRadarLog": search_response},
            )

        logger.warning(
            "Search Failed",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        return None

    except requests.exceptions.RequestException as e:
        logger.exception(
            "Search Error",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during QRadar search: {e}")


def _handle_successful_search(search_response, search_params, qradar_connector):
    """Handles the successful completion of a search."""
    try:
        parser_key_response_header = qradar_connector.get_parser_key(search_response)
        for key, value in parser_key_response_header.items():
            search_params["parser_key"] = key
            search_params["response_header"] = value
        logger.info(
            "Search Table Retrieved",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        return search_params

    except Exception as e:
        logger.error(
            "Search Result Processing Error",
            exc_info=True,
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        return None
