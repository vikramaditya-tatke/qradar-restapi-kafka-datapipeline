import requests.exceptions
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type, retry_if_exception,
)

from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import get_search_params
from settings import settings


def generate_search_params(event_processor, customer_name, query, duration):
    """Generates search parameters."""
    search_params_list = get_search_params(
        event_processor,
        customer_name,
        query,
        duration,
    )
    return search_params_list


# Custom retry condition function
def retry_if_not_unauthorized_error(exception):
    """Return True to retry if not a 401 Unauthorized error, False otherwise."""
    if isinstance(exception, requests.exceptions.HTTPError):
        if exception.response.status_code == 401:
            # Log the authentication error
            logger.error(
                f"Authentication failed with status code 401: {exception}",
                extra={"QRadarLog": {"Status Code": 401}},
            )
            return False  # Do not retry on 401 Unauthorized
    # Retry on other RequestExceptions
    return isinstance(exception, requests.exceptions.RequestException)

@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception(retry_if_not_unauthorized_error),
    retry_error_callback=lambda retry_state: logger.error(
        f"Trigger search retry attempt {retry_state.attempt_number} failed.",
        exc_info=True,
    ),
    reraise=True,
)
def trigger_search(qradar_connector, query_expression):
    """Triggers a search on the QRadar Console."""
    search_response = qradar_connector.trigger_search(query_expression)
    logger.info("Search Triggered", extra={"QRadarLog": search_response})
    return search_response


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error(
        f"Poll status retry attempt {retry_state.attempt_number} failed."
    ),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)
def poll_search_status(qradar_connector, cursor_id):
    """Polls the search status until completion or failure."""
    polling_response_header = qradar_connector.get_search_status(cursor_id)
    logger.info("Polling Search Status", extra={"cursor_id": cursor_id})
    return polling_response_header


def handle_search_success(polling_response_header, search_params, qradar_connector):
    """Handles the successful completion of a search."""
    try:
        parser_key_response_header = qradar_connector.get_parser_key(polling_response_header)
        for key, value in parser_key_response_header.items():
            search_params["parser_key"] = key
            search_params["response_header"] = value
        logger.info(
            "Search Completed Successfully",
            extra={"ApplicationLog": search_params, "QRadarLog": polling_response_header},
        )
        return search_params
    except Exception as e:
        logger.error(
            "Error retrieving search table",
            exc_info=True,
            extra={"ApplicationLog": search_params, "QRadarLog": polling_response_header},
        )
        return None


def handle_search_error(e, search_params, search_response):
    """Handles errors that occur during the search process."""
    if isinstance(e, KeyError):
        logger.warning(
            f"Search Failed - Incorrect AQL query format - {e}",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
    elif isinstance(e, requests.exceptions.HTTPError):
        error_code = e.response.status_code
        error_message = e.response.json().get('message', 'No error message provided')
        search_response["Status Code"] = error_code
        search_response["Error Message"] = error_message
        if 500 <= error_code < 600:
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
    else:
        logger.error(
            "Search Error - Unknown Error",
            exc_info=True,
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )


def search_executor(
    event_processor: int,
    customer_name: str,
    query: dict,
    duration: dict,
    qradar_connector: QRadarConnector,
):
    """
    Orchestrates the search execution process.
    """
    search_params_list = generate_search_params(
        event_processor,
        customer_name,
        query,
        duration,
    )
    search_response = None
    for search_params in search_params_list:
        try:
            logger.debug("Generated search parameters", extra={"ApplicationLog": search_params})

            # Trigger the search
            search_response = trigger_search(
                qradar_connector, search_params["query"]["query_expression"]
            )
            cursor_id = search_response["cursor_id"]
            search_params["attempt"] = 0

            # Poll the search status
            while search_params["attempt"] < settings.max_attempts:
                search_params["attempt"] += 1
                polling_response_header = poll_search_status(qradar_connector, cursor_id)
                logger.info(
                    "Search Status Polled",
                    extra={"ApplicationLog": search_params, "QRadarLog": polling_response_header},
                )

                if polling_response_header.get("completed"):
                    # Handle successful search
                    result = handle_search_success(
                        polling_response_header, search_params, qradar_connector
                    )
                    if result:
                        return result
                    else:
                        break  # Exit the loop if handling failed

                logger.info("Search is still running", extra={"cursor_id": cursor_id})

            else:
                logger.warning(
                    "Search Failed - Attempts Exhausted",
                    extra={"ApplicationLog": search_params, "QRadarLog": search_response},
                )
                return None

        except Exception as e:
            handle_search_error(e, search_params, search_response)
            # Decide whether to continue or break based on the exception
            if isinstance(e, requests.exceptions.HTTPError) and 500 <= e.response.status_code < 600:
                raise  # Re-raise server errors to trigger retries
            else:
                continue  # Skip to the next search_param if available

    return None
