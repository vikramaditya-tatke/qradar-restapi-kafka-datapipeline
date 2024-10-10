import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.search_executor import search_executor
from settings import Settings

# At the top of your module
executor = ThreadPoolExecutor(max_workers=10)


@dataclass
class QueryResult:
    event_processor: int
    customer_name: str
    query: Dict[str, str]
    duration: Dict[str, str]
    response_header: Dict[str, Any]
    attempt: int
    parser_key: str


async def execute_query(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    query: Dict[str, str],
    duration: Dict[str, str],
    semaphore: asyncio.Semaphore,
) -> Optional[QueryResult]:
    """Executes a single query asynchronously and returns the result."""
    async with semaphore:
        try:
            result = await asyncio.to_thread(
                search_executor,
                event_processor,
                customer_name,
                query,
                duration,
                qradar_connector,
            )
            if result and result["response_header"]["record_count"] > 0:
                logger.debug(
                    f"Query executed successfully for {customer_name}",
                    extra={"customer_name": customer_name, "query": query},
                )
                return QueryResult(
                    event_processor=event_processor,
                    customer_name=customer_name,
                    query=query,
                    duration=duration,
                    response_header=result["response_header"],
                    attempt=result["attempt"],
                    parser_key=result["parser_key"],
                )
            elif result:
                logger.info(
                    f"No records found for {customer_name}",
                    extra={"customer_name": customer_name, "query": query},
                )
                return None
        except Exception as e:
            logger.error(
                f"Error executing query for {customer_name}: {e}",
                exc_info=True,
                extra={"customer_name": customer_name, "query": query},
            )
            return None


async def process_etl(
    qradar_connector: QRadarConnector,
    result: QueryResult,
    semaphore: asyncio.Semaphore,
):
    """Processes ETL for a single result."""
    async with semaphore:
        try:
            response = await asyncio.to_thread(
                qradar_connector.fetch_data,
                result.response_header["cursor_id"],
                result.response_header["record_count"],
            )
            await asyncio.to_thread(
                etl,
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


async def process_customer(
    qradar_connector: QRadarConnector,
    event_processor: int,
    customer_name: str,
    queries: Dict[str, str],
    duration: Dict[str, str],
    query_semaphore: asyncio.Semaphore,
    etl_semaphore: asyncio.Semaphore,
):
    """Processes all queries for a single customer."""
    try:
        # Execute queries
        query_tasks: List[asyncio.Task] = []
        for name, expression in queries.items():
            task = asyncio.create_task(
                execute_query(
                    qradar_connector,
                    event_processor,
                    customer_name,
                    {"query_name": name, "query_expression": expression},
                    duration,
                    query_semaphore,
                )
            )
            query_tasks.append(task)

        results = await asyncio.gather(*query_tasks, return_exceptions=True)

        # Handle exceptions and collect successful results
        successful_results: List[QueryResult] = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    f"Query execution error for {customer_name}: {result}",
                    exc_info=True,
                    extra={"customer_name": customer_name},
                )
            elif isinstance(result, QueryResult):
                successful_results.append(result)
            elif result is None:
                logger.info(
                    f"No data returned for query for {customer_name}",
                    extra={"customer_name": customer_name},
                )
            else:
                logger.warning(
                    f"Unexpected result type for {customer_name}: {type(result)}",
                    extra={"customer_name": customer_name},
                )

        # Process ETL for successful results
        etl_tasks: List[asyncio.Task] = []
        for result in successful_results:
            task = asyncio.create_task(
                process_etl(qradar_connector, result, etl_semaphore)
            )
            etl_tasks.append(task)

        await asyncio.gather(*etl_tasks, return_exceptions=True)
    except Exception as e:
        logger.error(
            f"Error during processing for {customer_name}: {e}",
            exc_info=True,
            extra={"customer_name": customer_name},
        )
    finally:
        logger.info(
            f"Process completed for {customer_name}",
            extra={"customer_name": customer_name},
        )


async def process_all_customers(settings: Settings, console: str):
    """Processes all customers and event processors."""
    attributes = load_attributes()
    ep_client_list = attributes.get("ep_client_list")
    queries = attributes["queries"]
    duration = attributes["duration"]

    session = Session()

    console_mapping = {
        "1": "console_1",
        "2": "console_2",
        "3": "console_3",
        "aa": "console_aa",
        "aus": "console_aus",
        "uae": "console_uae",
        "us": "console_us",
    }

    console_attr = console_mapping.get(console)

    if console_attr is None:
        raise ValueError(
            f"Invalid console '{console}' specified. Available options: {list(console_mapping.keys())}"
        )

    token = getattr(settings, f"{console_attr}_token")
    ip = getattr(settings, f"{console_attr}_ip")

    qradar_connector = QRadarConnector(
        sec_token=token,
        session=session,
        base_url=f"https://{ip}",
    )

    # Semaphores to limit concurrency
    customer_semaphore = asyncio.Semaphore(5)  # Limit concurrent customers
    query_semaphore = asyncio.Semaphore(10)  # Limit concurrent queries per customer
    etl_semaphore = asyncio.Semaphore(5)  # Limit concurrent ETL processes

    tasks: List[asyncio.Task] = []

    for ep, customer_names in ep_client_list.items():
        for customer_name in customer_names:
            async with customer_semaphore:
                task = asyncio.create_task(
                    process_customer(
                        qradar_connector,
                        event_processor=ep,
                        customer_name=customer_name,
                        queries=queries,
                        duration=duration,
                        query_semaphore=query_semaphore,
                        etl_semaphore=etl_semaphore,
                    )
                )
                tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle exceptions from customer processing
    for result in results:
        if isinstance(result, Exception):
            logger.error(
                f"Error processing customer: {result}",
                exc_info=True,
            )


async def main():
    """Main entry point for the ETL process."""
    parser = argparse.ArgumentParser(description="Run the QRadar ETL pipeline")
    parser.add_argument(
        "--console",
        type=str,
        required=False,
        help="Specify the QRadar console to use (e.g., 1, us, uae)",
    )
    args = parser.parse_args()

    logger.debug("Application Started")
    settings = Settings()

    try:
        await process_all_customers(settings, args.console)
    except Exception as e:
        logger.critical(
            f"Unexpected error during main execution: {e}",
            exc_info=True,
        )
    finally:
        pending = asyncio.all_tasks()
        if pending:
            logger.debug(f"Pending tasks at exit")
        else:
            logger.debug("No pending tasks.")
        logger.debug("Exiting program")


if __name__ == "__main__":
    asyncio.run(main())
    executor.shutdown(wait=True)
