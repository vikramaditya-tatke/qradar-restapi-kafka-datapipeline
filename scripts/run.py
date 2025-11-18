import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from multiprocessing import Pool

from requests import Session
from requests.exceptions import (
    Timeout,
    ConnectionError,
    HTTPError,
    RequestException,
)
from src.models.attributes import load_attributes
from src.pipeline.transformer import etl
from src.utils.logger import logger
from src.clients.qradar import QRadarConnector
from src.pipeline.executor import search_executor
from src.utils.config import Settings


@dataclass
class QueryResult:
    """Result container for completed QRadar query execution.

    Attributes:
        event_processor: QRadar event processor ID.
        customer_name: Target customer identifier.
        query: Query name and AQL expression.
        duration: Search time range configuration.
        response_header: QRadar API response metadata.
        attempt: Number of retry attempts made.
        parser_key: JSON parsing key for streaming results.
    """

    event_processor: int
    customer_name: str
    query: dict
    duration: dict
    response_header: dict
    attempt: int
    parser_key: str


def process_query(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    query: dict[str, str],
    duration: dict[str, str],
) -> QueryResult | None:
    """Executes QRadar search and triggers ETL if results contain data.

    Args:
        qradar_connector: Authenticated QRadar API client.
        event_processor: Target event processor ID.
        customer_name: Customer identifier for filtering.
        query: Query name and AQL expression mapping.
        duration: Start and stop time configuration.

    Returns:
        QueryResult if processing succeeds, None if failed or no data.
    """
    # Prepare logging context for all operations
    log_context = {
        "ApplicationLog": {
            "event_processor": event_processor,
            "customer_name": customer_name,
            "query_name": query.get("query_name", "unknown"),
            "start_time": duration.get("start_time"),
            "stop_time": duration.get("stop_time"),
        }
    }
    result = {}
    try:
        # Execute the query
        result = search_executor(
            event_processor, customer_name, query, duration, qradar_connector
        )

        if result and result["response_header"]["record_count"] > 0:
            # Prepare the result for further ETL processing
            query_result = QueryResult(
                event_processor=event_processor,
                customer_name=customer_name,
                query=query,
                duration=duration,
                response_header=result["response_header"],
                attempt=result.get("attempt", 0),
                parser_key=result.get("parser_key", ""),
            )

            # Add record count to logging context
            log_context["ApplicationLog"]["record_count"] = result["response_header"][
                "record_count"
            ]
            logger.info("Query executed successfully, starting ETL", extra=log_context)

            # Run ETL for this query
            process_etl(qradar_connector, query_result)
            return query_result

        else:
            logger.warning("No records found in QRadar response", extra=log_context)
            return None

    except Timeout as e:
        logger.error(
            "QRadar API request timeout",
            exc_info=True,
            extra={**log_context, "QRadarLog": {"timeout_error": str(e)}},
        )
        return None

    except ConnectionError as e:
        logger.error(
            "QRadar API connection failed",
            exc_info=True,
            extra={**log_context, "QRadarLog": {"connection_error": str(e)}},
        )
        return None

    except HTTPError as e:
        logger.error(
            "QRadar API HTTP error",
            exc_info=True,
            extra={
                **log_context,
                "QRadarLog": {
                    "http_status": e.response.status_code if e.response else None,
                    "http_error": str(e),
                },
            },
        )
        return None

    except RequestException as e:
        logger.error(
            "QRadar API request failed",
            exc_info=True,
            extra={**log_context, "QRadarLog": {"request_error": str(e)}},
        )
        return None

    except KeyError as e:
        logger.error(
            "Missing expected field in QRadar response",
            exc_info=True,
            extra={
                **log_context,
                "QRadarLog": {
                    "missing_field": str(e),
                    "result_keys": list(result.keys()) if result else [],
                },
            },
        )
        return None

    except ValueError as e:
        logger.error(
            "Invalid data format from QRadar",
            exc_info=True,
            extra={**log_context, "QRadarLog": {"data_error": str(e)}},
        )
        return None

    except Exception as e:
        # Only for truly unexpected errors - these should be investigated
        logger.critical(
            "Unexpected error in process_query",
            exc_info=True,
            extra={
                **log_context,
                "QRadarLog": {
                    "unexpected_error": str(e),
                    "error_type": type(e).__name__,
                },
            },
        )
        raise  # Re-raise unexpected errors for proper debugging


def process_etl(qradar_connector: QRadarConnector, result: QueryResult):
    """Fetches QRadar data and executes ETL pipeline to ClickHouse.

    Args:
        qradar_connector: Authenticated QRadar API client.
        result: Query execution result containing metadata and identifiers.
    """
    search_params = {
        "event_processor": int(result.event_processor),
        "customer_name": result.customer_name,
        "query": result.query,
        "response_header": result.response_header,
        "attempt": result.attempt,
        "parser_key": result.parser_key,
        "start_time": result.duration["start_time"],
        "stop_time": result.duration["stop_time"],
    }
    try:
        response = qradar_connector.fetch_data(
            result.response_header["cursor_id"],
            result.response_header["record_count"],
        )
        etl(
            response=response,
            search_params=search_params,
            base_url=qradar_connector.base_url,
        )
    except Exception:
        logger.error(
            "ETL process failed",
            exc_info=True,
            extra={
                "ApplicationLog": search_params,
            },
        )


def process_customer(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    queries: dict[str, str],
    duration: dict[str, str],
    max_threads: int,
):
    """Executes all configured queries for a customer using concurrent threads.

    Args:
        qradar_connector: Authenticated QRadar API client.
        event_processor: Target event processor ID.
        customer_name: Customer identifier for processing.
        queries: Mapping of query names to AQL expressions.
        duration: Time range configuration for all queries.
        max_threads: Maximum concurrent query threads.
    """
    try:
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submit each query to a thread
            future_to_query = {
                executor.submit(
                    process_query,
                    qradar_connector,
                    event_processor,
                    customer_name,
                    {"query_name": name, "query_expression": expression},
                    duration,
                ): name
                for name, expression in queries.items()
            }

            # Collect the results as they complete
            for future in as_completed(future_to_query):
                query_name = future_to_query[future]
                try:
                    future.result()  # We call result() to raise any exceptions
                except Exception as e:
                    logger.error(
                        f"Query execution error for {customer_name} on query {query_name}: {e}",
                        exc_info=True,
                        extra={"customer_name": customer_name},
                    )

    except Exception as e:
        logger.error(
            f"Error processing customer {customer_name}: {e}",
            exc_info=True,
            extra={"customer_name": customer_name},
        )
    finally:
        logger.info(
            f"Finished processing customer {customer_name}",
            extra={"customer_name": customer_name},
        )


def process_event_processor(
    ep: int,
    customers: list,
    queries: dict,
    duration: dict,
    token: str,
    ip: str,
    max_threads: int,
):
    """Processes all customers for an event processor using dedicated connection.

    Args:
        ep: Event processor ID to target.
        customers: List of customer names to process.
        queries: Query name to AQL expression mapping.
        duration: Time range configuration.
        token: QRadar API authentication token.
        ip: QRadar console IP address.
        max_threads: Maximum threads per customer.
    """
    session = Session()
    qradar_connector = QRadarConnector(
        sec_token=token,
        session=session,
        base_url=f"https://{ip}",
    )

    for customer_name in customers:
        process_customer(
            qradar_connector, ep, customer_name, queries, duration, max_threads
        )


def process_console(console_attr: str, max_threads: int):
    """Orchestrates multiprocess execution across all event processors for a console.

    Args:
        console_attr: Console configuration attribute name (e.g., 'console_1').
        max_threads: Maximum threads per event processor.
    """
    settings = Settings.model_validate({})
    attributes = load_attributes()
    ep_client_list = attributes["ep_client_list"]
    queries = attributes["queries"]
    duration = attributes["duration"]

    # Retrieve token and IP for the specified console
    token = getattr(settings, f"{console_attr}_token")
    ip = getattr(settings, f"{console_attr}_ip")

    # Create arguments for multiprocessing
    etl_params = [
        (ep, customers, queries, duration, token, ip, max_threads)
        for ep, customers in ep_client_list
    ]

    # Process each EP using multiprocessing
    with Pool(processes=len(ep_client_list)) as pool:
        pool.starmap(process_event_processor, etl_params)


def main():
    """CLI entry point for QRadar data pipeline execution.

    Parses command line arguments and initiates pipeline processing
    for the specified console with configured threading limits.
    """

    parser = argparse.ArgumentParser(
        description="Run the QRadar ETL pipeline for a specific console"
    )
    parser.add_argument(
        "--console",
        type=str,
        required=True,
        help="Specify the QRadar console to use (e.g., 1, us, uae)",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=5,
        help="Specify the maximum number of threads per event processor (default is 5)",
    )
    args = parser.parse_args()

    logger.debug("Application Started")

    # Console mapping
    console_mapping = {
        "1": "console_1",
        "2": "console_2",
        "3": "console_3",
        "aa": "console_aa",
        "aus": "console_aus",
        "uae": "console_uae",
        "us": "console_us",
    }

    # Validate and retrieve console attributes
    try:
        # Process all event processors for the given console
        console_attr = console_mapping.get(args.console)
        if console_attr is None:
            raise ValueError(
                f"Invalid console '{args.console}' specified. Available options: {list(console_mapping.keys())}"
            )
        process_console(console_attr, args.max_threads)
    except Exception as e:
        logger.critical(
            f"Unexpected error during main execution: {e}",
            exc_info=True,
        )
    finally:
        logger.debug("Exiting program")


if __name__ == "__main__":
    main()
