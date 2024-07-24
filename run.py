import multiprocessing as mp

from requests import Session

from attributes import load_attributes
from etl import etl
from pipeline_logger import logger
from qradar.qradarconnector import QRadarConnector
from qradar.query_builder import construct_base_urls
from qradar.search_executor import search_executor
from settings import settings


def starter(event_processor, customer_name):
    # Initialize per process to ensure thread safety
    event_processor = [int(event_processor)]
    queries = load_attributes()["queries"]
    session = Session()
    base_url = construct_base_urls()
    qradar_connector = QRadarConnector(
        sec_token=settings.console_3_token,
        session=session,
        base_url=base_url["console_3"],
    )
    search_params = []
    for query in queries.items():
        search_params.append(
            (event_processor[0], customer_name[0], query, qradar_connector)
        )
    for param in search_params:
        result = search_executor(param)
        etl(session=session, search_params=result, base_url=base_url["console_3"])
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     futures = executor.map(
    #         lambda params: search_executor(*params),
    #         search_params,
    #     )
    #
    # for result in futures:
    #     etl(session=session, search_params=result, base_url=base_url["console_3"])


if __name__ == "__main__":
    logger.debug("Application Started")
    ep_client_list = load_attributes().get("ep_client_list")
    with mp.Pool(processes=2) as pool:  # Adjust number of processes as needed
        pool.starmap(
            starter,
            zip(
                list(ep_client_list.keys()),
                ep_client_list.values(),
            ),
        )

    logger.debug("All processes finished")  # Added a log message for completeness
