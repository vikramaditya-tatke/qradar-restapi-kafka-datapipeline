import requests.exceptions
from tenacity import stop_after_attempt, wait_exponential, retry

from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
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
def search_executor(
    event_processor: int,
    customer_name: str,
    query: dict,
    qradar_connector: QRadarConnector,
):
    """
    Performs a QRadar search with retry mechanism and detailed logging.
    Creates a process for each console.
    """
    search_response = {}
    # Generate the search parameters and split the query according to fixed time intervals if query is of large or
    # medium type.
    search_params, split_query = get_search_params(
        event_processor,
        customer_name,
        query,
    )
    try:
        logger.debug(
            "Generated search parameters",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": search_response,
            },
        )

        # Trigger a search on the QRadar Console with the parameters generated above.
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

        # Extract the `cursor_id` from the response and add the attempt number to the search_params dictionary.
        cursor_id = search_response["cursor_id"]
        search_params["attempt"] = 0

        # Poll using the `cursor_id` until the search is complete or the attempts are exhausted or the search fails.
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

            # Handle a successfully completed search.
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

            # Continue with the next iteration of the loop if the polling_response_header has completed == False.
            logger.info(
                "Search Running",
                extra={"ApplicationLog": search_params, "QRadarLog": search_response},
            )

        # Log a warning if the attempts are exhausted.
        logger.warning(
            "Search Failed - Attempts Exhausted",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        return None
    except KeyError as ke:
        logger.warning(
            f"Search Failed - Incorrect AQL query format - {ke}",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
    except requests.exceptions.HTTPError as req_err:
        search_response["QRadar Error Code"] = (
            req_err.response.json().get("http_response").get("code")
        )
        search_response["QRadar Error Message"] = (
            f"{req_err.response.json().get('http_response').get('message')} because {req_err.response.json().get('message')}"
        )
        search_response["Status Code"] = req_err.response.status_code
        if 500 <= req_err.response.status_code < 600:
            logger.error(
                "Search Error - Server Error",
                extra={"ApplicationLog": search_params, "QRadarLog": search_response},
            )
            raise
        else:
            logger.error(
                "Search Error - Client Error",
                extra={"ApplicationLog": search_params, "QRadarLog": search_response},
            )
    except Exception:
        logger.error(f"Search Error - Unknown Error")


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

    except Exception:
        logger.error(
            "Search Error - Error retrieving Search Table",
            exc_info=True,
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
        return None
