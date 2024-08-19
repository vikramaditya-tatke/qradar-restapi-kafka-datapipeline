import concurrent.futures
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import clickhouse_connect.driver.exceptions
from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import construct_base_urls
from qradar.search_executor import search_executor
from settings import settings


class QRadarProcessor:
    def __init__(self, event_processor, customer_names, queries):
        self.event_processor = event_processor
        self.customer_names = customer_names
        self.queries = queries
        self.qradar_connector = self.setup_qradar_connector()

    @staticmethod
    def setup_qradar_connector():
        """Initializes the QRadarConnector."""
        session = Session()
        base_url = construct_base_urls()
        return QRadarConnector(
            sec_token=settings.console_1_token,
            session=session,
            base_url=base_url["console_1"],
        )

    def execute_queries(self):
        """Executes the search queries and returns the results."""
        search_params = [
            (self.event_processor, customer_name, query, self.qradar_connector)
            for customer_name in self.customer_names
            for query in self.queries.items()
        ]

        results = []
        with ThreadPoolExecutor(
            max_workers=settings.max_queries_per_event_processor
        ) as executor:
            future_to_query = {
                executor.submit(search_executor, param): param
                for param in search_params
            }
            for future in as_completed(future_to_query):
                try:
                    result = future.result()
                    if result:
                        if result["response_header"]["record_count"] == 0:
                            logger.info("Search Result Empty", extra=result)
                            continue
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error processing query: {e}", exc_info=True)
        return results

    def process_customer(self, customer_name):
        """Processes a single customer's events."""
        try:
            results = self.execute_queries()
            if len(results) > 0:
                for result in results:
                    if result:  # Ensure the result is not None or invalid
                        try:
                            response = self.qradar_connector.fetch_data(
                                result["response_header"]["cursor_id"],
                                result["response_header"]["record_count"],
                            )
                            etl(
                                response=response,
                                search_params=result,
                                base_url=self.qradar_connector.base_url,
                            )
                        except clickhouse_connect.driver.exceptions.DataError as e:
                            logger.error(
                                f"Data type mismatch error for {customer_name}: {e}"
                            )
                            raise
                        except Exception as e:
                            logger.error(
                                f"ETL process failed for {customer_name}: {e}",
                                exc_info=True,
                            )
                            raise
        except Exception as e:
            logger.error(
                f"Error during processing for {customer_name}: {e}", exc_info=True
            )
            return False
        finally:
            logger.info(
                f"Process Completed",
                extra={"ApplicationLog": {"customer_name": customer_name}},
            )


def handle_customer(event_processor, customer_name, queries):
    processor = QRadarProcessor(event_processor, [customer_name], queries)
    processor.process_customer(customer_name)


def graceful_exit(signum, frame):
    logger.info("Received termination signal. Exiting gracefully...")
    sys.exit(0)


# TODO: Implement better force shutdown, and interrupt handling.
# TODO: Implement parallel execution of the `etl` function.
def main():
    logger.debug("Application Started")
    attributes = load_attributes()
    ep_client_list = attributes.get("ep_client_list")
    queries = attributes["queries"]

    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    try:
        with ProcessPoolExecutor(
            max_workers=settings.max_event_processors_engaged
        ) as executor:
            futures = {
                executor.submit(
                    handle_customer, ep, customer_name, queries
                ): customer_name
                for ep, customer_names in ep_client_list.items()
                for customer_name in customer_names
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in processing: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.critical("User Interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        logger.debug("Exiting program")


if __name__ == "__main__":
    main()
