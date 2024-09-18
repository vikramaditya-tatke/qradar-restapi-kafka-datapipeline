import argparse
import asyncio
from dataclasses import dataclass
from typing import List, Dict, Any
from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import construct_base_urls
from qradar.search_executor import search_executor
from settings import Settings


@dataclass
class QueryResult:
    event_processor: int
    customer_name: str
    query: Dict[str, str]
    response_header: Dict[str, Any]
    attempt: int
    parser_key: str


class QueryExecutor:
    """Class responsible for executing QRadar queries."""

    def __init__(self, qradar_connector: QRadarConnector):
        self.qradar_connector = qradar_connector

    # TODO: Make the exception clauses more specific.
    async def execute(self, event_processor: int, customer_name: str, query: Dict[str, str]) -> QueryResult:
        """Executes a single query asynchronously and returns the result."""
        try:
            result = await asyncio.to_thread(
                search_executor,
                event_processor,
                customer_name,
                query,
                self.qradar_connector,
            )
            if result and result["response_header"]["record_count"] > 0:
                return QueryResult(
                    event_processor=event_processor,
                    customer_name=customer_name,
                    query=query,
                    response_header=result["response_header"],
                    attempt=result["attempt"],
                    parser_key=result["parser_key"],
                )
            elif result:
                logger.info("Search result empty", extra=result)

        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            raise


class ETLProcessor:
    """Class responsible for handling the ETL process."""

    def __init__(self, qradar_connector: QRadarConnector):
        self.qradar_connector = qradar_connector

    async def process(self, result: QueryResult):
        """Processes ETL for a single result."""
        try:
            response = await asyncio.to_thread(
                self.qradar_connector.fetch_data,
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
                base_url=self.qradar_connector.base_url,
            )
        except Exception as e:
            logger.error(f"ETL process failed for {result.customer_name}: {e}", exc_info=True)


class QRadarProcessor:
    """Orchestrates the processing of customers and event processors."""

    def __init__(
            self, event_processor: int, customer_names: List[str], queries: Dict[str, str],
            query_executor: QueryExecutor, etl_processor: ETLProcessor
            ):
        self.event_processor = event_processor
        self.customer_names = customer_names
        self.queries = queries
        self.query_executor = query_executor
        self.etl_processor = etl_processor

    async def process_customer(self, customer_name: str):
        """Processes all queries for a single customer."""
        try:
            query_tasks = [
                self.query_executor.execute(
                    self.event_processor,
                    customer_name,
                    {"query_name": name, "query_expression": expression}
                )
                for name, expression in self.queries.items()
            ]

            results = await asyncio.gather(*query_tasks)
            results = [result for result in results if result]

            etl_tasks = [self.etl_processor.process(result) for result in results]
            await asyncio.gather(*etl_tasks)

        except Exception as e:
            logger.error(f"Error during processing for {customer_name}: {e}", exc_info=True)
        finally:
            logger.info("Process Completed", extra={"customer_name": customer_name})


async def process_all_customers(settings: Settings, console: str):
    """Processes all customers and event processors."""
    attributes = load_attributes()
    ep_client_list = attributes.get("ep_client_list")
    queries = attributes["queries"]

    # Directly create the QRadarConnector based on the console
    session = Session()
    base_url = construct_base_urls(settings)

    console_mapping = {
        '1': 'console_1',
        '2': 'console_2',
        '3': 'console_3',
        'aa': 'console_aa',
        'uae': 'console_uae',
        'us': 'console_us'
    }

    console_attr = console_mapping.get(console)

    if console_attr is None:
        raise ValueError(f"Invalid console '{console}' specified. Available options: {list(console_mapping.keys())}")

    token = getattr(settings, f'{console_attr}_token')
    ip = getattr(settings, f'{console_attr}_ip')

    qradar_connector = QRadarConnector(
        sec_token=token,
        session=session,
        base_url=f"https://{ip}",
    )

    query_executor = QueryExecutor(qradar_connector)
    etl_processor = ETLProcessor(qradar_connector)

    tasks = [
        QRadarProcessor(ep, customer_names, queries, query_executor, etl_processor).process_customer(customer_name)
        for ep, customer_names in ep_client_list.items()
        for customer_name in customer_names
    ]

    await asyncio.gather(*tasks)


async def main():
    """Main entry point for the ETL process."""

    parser = argparse.ArgumentParser(description="Run the QRadar ETL pipeline.")
    parser.add_argument("--console", type=str, required=True,
                        help="Specify the QRadar console to use (e.g., 1, us, uae)")
    args = parser.parse_args()

    logger.debug("Application Started")
    settings = Settings()

    try:
        await process_all_customers(settings, args.console)

    except Exception as e:
        logger.critical(f"Unexpected error during main execution: {e}", exc_info=True)

    finally:
        logger.debug("Exiting program")


if __name__ == "__main__":
    asyncio.run(main())
