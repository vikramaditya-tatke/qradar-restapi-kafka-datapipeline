from pipeline_logger import logger
from qradar.search_executor import search_executor
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from attributes import load_attributes


def starter(event_processor, customer_name):
    # Initialize per process to ensure thread safety
    try:
        event_processor = [int(event_processor)]
        queries = load_attributes()["queries"]
        search_params = zip(
            event_processor,
            customer_name,
            queries.items(),
        )
        with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust worker count
            results = list(
                executor.map(
                    search_executor,
                    search_params,
                )
            )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    logger.debug("Application Started")
    ep_client_list = load_attributes().get("ep_client_list")
    with mp.Pool(processes=5) as pool:  # Adjust number of processes as needed
        pool.starmap(
            starter,
            zip(
                list(ep_client_list.keys()),
                ep_client_list.values(),
            ),
        )

    logger.debug("All processes finished")  # Added a log message for completeness
