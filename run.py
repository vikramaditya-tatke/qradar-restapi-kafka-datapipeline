from qradarconnector import QRadarConnector
from settings import settings
from pipeline_logger import logger
from producer import create_producer
from query_builder import construct_base_urls, get_search_params
from search_executor import perform_qradar_searches
import multiprocessing as mp


def starter(base_url, search_params):
    # Initialize per process to ensure thread safety
    producer = create_producer()
    qradar_connector = QRadarConnector(sec_token=settings.console_3_token, base_url=base_url)
    polling_response_header = perform_qradar_searches(qradar_connector, search_params=search_params)

    if polling_response_header:
        logger.info(f"Started Data Fetching from {base_url}")
        qradar_connector.receive_data(polling_response_header, producer, search_params)
    else:
        logger.warning(f"Search failed after maximum attempts for {base_url}")


if __name__ == "__main__":
    logger.debug("Application Started")
    base_url = construct_base_urls()
    logger.debug("Constructed base_urls")
    search_params = get_search_params()
    logger.debug("Generated search parameters")

    with mp.Pool(processes=8) as pool:  # Adjust number of processes as needed
        pool.map(starter, (base_url, search_params))

    logger.debug("All processes finished")  # Added a log message for completeness
