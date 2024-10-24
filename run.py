import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from multiprocessing import Pool
from typing import Dict, Any, Optional, List

from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.search_executor import search_executor
from settings import Settings


@dataclass
class QueryResult:
    event_processor: int
    customer_name: str
    query: Dict[str, str]
    duration: Dict[str, str]
    response_header: Dict[str, Any]
    attempt: int
    parser_key: str


def process_query(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    query: Dict[str, str],
    duration: Dict[str, str],
) -> Optional[QueryResult]:
    """Process a single query and execute ETL if query has data."""
    try:
        # Execute the query
        result = search_executor(
            event_processor, customer_name, query, duration, qradar_connector
        )

        if result and result["response_header"]["record_count"] > 0:
            logger.debug(
                f"Query executed successfully for {customer_name}",
                extra={"customer_name": customer_name, "query": query},
            )

            # Remove `.` and `'` from the customer_name
            # customer_name = customer_name.replace(".", "").replace("'", "")
            # Prepare the result for further ETL processing
            query_result = QueryResult(
                event_processor=event_processor,
                customer_name=customer_name,
                query=query,
                duration=duration,
                response_header=result["response_header"],
                attempt=result["attempt"],
                parser_key=result["parser_key"],
            )

            # Run ETL for this query
            process_etl(qradar_connector, query_result)

        elif result:
            logger.info(
                f"No records found for {customer_name}",
                extra={"customer_name": customer_name, "query": query},
            )

    except Exception as e:
        logger.error(
            f"Error processing query for {customer_name}: {e}",
            exc_info=True,
            extra={"customer_name": customer_name, "query": query},
        )
        return None


def process_etl(qradar_connector: QRadarConnector, result: QueryResult):
    """Processes ETL for a single result."""
    try:
        response = qradar_connector.fetch_data(
            result.response_header["cursor_id"],
            result.response_header["record_count"],
        )
        etl(
            response=response,
            search_params={
                "event_processor": int(result.event_processor),
                "customer_name": result.customer_name,
                "query": result.query,
                "response_header": result.response_header,
                "attempt": result.attempt,
                "parser_key": result.parser_key,
            },
            base_url=qradar_connector.base_url,
        )
        logger.info(
            f"ETL process completed for {result.customer_name}",
            extra={"customer_name": result.customer_name},
        )
    except Exception as e:
        logger.error(
            f"ETL process failed for {result.customer_name}: {e}",
            exc_info=True,
            extra={"customer_name": result.customer_name},
        )


def process_customer(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    queries: Dict[str, str],
    duration: Dict[str, str],
    max_threads: int,
):
    """Processes all queries for a single customer using threads."""
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
    customers: List[str],
    queries: Dict[str, str],
    duration: Dict[str, str],
    token: str,
    ip: str,
    max_threads: int,
):
    """Processes all customers for a given event processor (EP) in a single process."""
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
    """Processes all event processors for a given console."""
    settings = Settings()
    attributes = load_attributes()
    ep_client_list = attributes.get("ep_client_list")
    queries = attributes["queries"]
    duration = attributes["duration"]

    # Retrieve token and IP for the specified console
    token = getattr(settings, f"{console_attr}_token")
    ip = getattr(settings, f"{console_attr}_ip")

    # Create arguments for multiprocessing
    etl_params = [
        (ep, customers, queries, duration, token, ip, max_threads)
        for ep, customers in ep_client_list.items()
    ]

    # Process each EP using multiprocessing
    with Pool(processes=len(ep_client_list)) as pool:
        pool.starmap(process_event_processor, etl_params)


def main():
    """Main entry point for the ETL process."""

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
