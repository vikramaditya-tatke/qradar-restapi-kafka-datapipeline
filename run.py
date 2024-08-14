import multiprocessing as mp
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import construct_base_urls
from qradar.search_executor import search_executor
from settings import settings


def graceful_exit(signal, frame):
    """Handles graceful exit on receiving termination signals."""
    logger.info("Received termination signal. Exiting gracefully...")
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, graceful_exit)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, graceful_exit)  # Handle termination


def setup_qradar_connector():
    """Initializes the QRadarConnector."""
    session = Session()
    base_url = construct_base_urls()
    qradar_connector = QRadarConnector(
        sec_token=settings.console_3_token,
        session=session,
        base_url=base_url["console_3"],
    )
    return qradar_connector, session


def execute_queries(event_processor, customer_name, queries, qradar_connector):
    """Executes the search queries and performs ETL."""
    search_params = [
        (event_processor, customer_name, query, qradar_connector)
        for query in queries.items()
    ]

    results = []
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        future_to_query = {
            executor.submit(search_executor, param): param for param in search_params
        }
        for future in as_completed(future_to_query):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing query: {e}", exc_info=True)

    return results


def starter(event_processor, customers):
    """Main entry point for processing each customer's events."""

    event_processor = int(event_processor)
    queries = load_attributes()["queries"]
    qradar_connector, session = setup_qradar_connector()

    for customer_name in customers:
        try:
            results = execute_queries(
                event_processor, customer_name, queries, qradar_connector
            )

            # ETL for concurrently fetched results
            for result in results:
                if result:  # Ensure the result is not None or invalid
                    etl(
                        session=session,
                        search_params=result,
                        base_url=qradar_connector.base_url,
                    )

        except Exception as e:
            logger.error(
                f"Error during processing for {customer_name}: {e}", exc_info=True
            )
        finally:
            logger.info(f"Process for {customer_name} finished.")


if __name__ == "__main__":
    logger.debug("Application Started")
    ep_client_list = load_attributes().get("ep_client_list")

    try:
        with mp.Pool(processes=8) as pool:  # Adjust number of processes as needed
            pool.starmap(
                starter,
                zip(
                    ep_client_list.keys(),
                    ep_client_list.values(),
                ),
            )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Terminating processes...")
        pool.terminate()
        pool.join()
        logger.info("All processes terminated gracefully.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        logger.debug("All processes finished. Exiting program.")
