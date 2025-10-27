import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_exception,
)

from src.utils.logger import logger
from src.clients.qradar import QRadarConnector
from src.pipeline.query_builder import get_search_params
from src.utils.config import settings

# Define common status codes
UNAUTHORIZED = 401
UNPROCESSABLE_ENTITY = 422
SERVER_ERROR_RANGE = range(500, 600)


def generate_search_params(event_processor, customer_name, query, duration):
    """Generates a list of search parameters."""
    return get_search_params(event_processor, customer_name, query, duration)


# Helper Functions for Status Code Checks
def is_unauthorized(exception):
    """Check if the exception is a 401 Unauthorized error."""
    return (
        isinstance(exception, requests.exceptions.HTTPError)
        and exception.response.status_code == UNAUTHORIZED
    )


def is_unprocessable_entity(exception):
    """Check if the exception is a 422 Unprocessable Entity error."""
    return (
        isinstance(exception, requests.exceptions.HTTPError)
        and exception.response.status_code == UNPROCESSABLE_ENTITY
    )


def is_server_error(exception):
    """Check if the exception is a server error (5xx)."""
    return (
        isinstance(exception, requests.exceptions.HTTPError)
        and exception.response.status_code in SERVER_ERROR_RANGE
    )


# Custom retry condition function
def handle_client_error_retries(exception):
    """
    Determine whether to retry based on the exception type and status code.

    Returns:
        bool: True if the operation should be retried, False otherwise.
    """
    if is_unauthorized(exception):
        logger.error(
            "Authentication failed with status code 401.",
            extra={"QRadarLog": exception.response.json()},
        )
        return False
    elif is_unprocessable_entity(exception):
        # logger.error(
        #     "Syntax error due to incorrect query, EP, or customer name.",
        #     extra={"QRadarLog": exception.response.json()},
        # )
        return False
    return isinstance(exception, requests.exceptions.RequestException)


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception(handle_client_error_retries),
    retry_error_callback=lambda retry_state: logger.error(
        f"Trigger search retry attempt {retry_state.attempt_number} failed.",
        exc_info=True,
    ),
    reraise=True,
)
def trigger_search(qradar_connector, query_expression):
    """Triggers a search on the QRadar Console."""
    return qradar_connector.trigger_search(query_expression)


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    retry_error_callback=lambda retry_state: logger.error(
        f"Poll status retry attempt {retry_state.attempt_number} failed.",
        exc_info=True,
    ),
    reraise=True,
)
def poll_search_status(qradar_connector, cursor_id):
    """Polls the search status until completion or failure."""
    return qradar_connector.get_search_status(cursor_id)


def handle_search_success(polling_response, search_params, qradar_connector):
    """Handles the successful completion of a search."""
    try:
        parser_key_response_header = qradar_connector.get_parser_key(polling_response)
        for key, value in parser_key_response_header.items():
            search_params["parser_key"] = key
            search_params["response_header"] = value
        logger.info(
            "Search completed successfully.",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": polling_response,
            },
        )
        return search_params
    except Exception as e:
        logger.error(
            "Error retrieving search table.",
            exc_info=True,
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": polling_response,
            },
        )
        return None


def handle_search_error(exception, search_params, search_response):
    """Handles errors that occur during the search process."""
    if isinstance(exception, KeyError):
        logger.warning(
            "Search failed due to incorrect AQL query format.",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
    elif isinstance(exception, ValueError):
        logger.error(
            "Search failed due to incorrect or missing search parameters.",
            extra={"ApplicationLog": search_params, "QRadarLog": search_response},
        )
    elif isinstance(exception, requests.exceptions.HTTPError):
        error_info = {
            "Status Code": exception.response.status_code,
            "Error Message": exception.response.json().get(
                "message", "No error message provided"
            ),
        }
        search_response.update(error_info)
        if is_server_error(exception):
            logger.error(
                "Server error during search.",
                extra={
                    "ApplicationLog": search_params,
                    "QRadarLog": exception.response.json(),
                },
            )
            raise  # Re-raise to trigger retry
        else:
            logger.error(
                "Client error during search.",
                extra={
                    "ApplicationLog": search_params,
                    "QRadarLog": exception.response.json(),
                },
            )
    else:
        logger.error(
            "Unknown error during search.",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": search_response,
            },
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

    Args:
        event_processor (int): The event processor identifier.
        customer_name (str): The name of the customer.
        query (dict): The search query parameters.
        duration (dict): The duration for the search.
        qradar_connector (QRadarConnector): The QRadar connector instance.

    Returns:
        dict or None: The search results if successful, otherwise None.
    """
    search_params_list = generate_search_params(
        event_processor,
        customer_name,
        query,
        duration,
    )

    for search_params in search_params_list:
        logger.debug(
            "Generated search parameters.",
            extra={"ApplicationLog": search_params},
        )
        search_response = {}
        try:
            # Trigger the search
            search_response = trigger_search(
                qradar_connector, search_params["query"]["query_expression"]
            )
            if not search_response:
                logger.warning(
                    "No search response received.",
                    extra={"ApplicationLog": search_params},
                )
                continue

            logger.info(
                "Search triggered successfully.",
                extra={
                    "ApplicationLog": search_params,
                    "QRadarLog": search_response,
                },
            )

            cursor_id = search_response.get("cursor_id")
            if not cursor_id:
                logger.error(
                    "No cursor_id found in search response.",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": search_response,
                    },
                )
                continue

            search_params["attempt"] = 0

            # Poll the search status
            while search_params["attempt"] < settings.max_attempts:
                search_params["attempt"] += 1
                logger.info(
                    f"Polling search status",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": search_response,
                    },
                )

                polling_response = poll_search_status(qradar_connector, cursor_id)
                logger.info(
                    "Search status polled.",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": polling_response,
                    },
                )

                if polling_response.get("completed"):
                    # Handle successful search
                    result = handle_search_success(
                        polling_response, search_params, qradar_connector
                    )
                    if result:
                        return result
                    break  # Exit the loop if handling failed

                logger.info(
                    "Search is still running.",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": polling_response,
                    },
                )

            else:
                logger.warning(
                    "Search failed after maximum attempts.",
                    extra={
                        "ApplicationLog": search_params,
                        "QRadarLog": search_response,
                    },
                )

        except Exception as e:
            handle_search_error(e, search_params, search_response)
            # Re-raise only if it's a server error to trigger retry
            if isinstance(e, requests.exceptions.HTTPError) and is_server_error(e):
                raise  # Re-raise to trigger tenacity retries
            # For other exceptions, continue to the next search_params
            continue

    return None
