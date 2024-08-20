from datetime import datetime, timedelta

from pipeline_logger import logger
from settings import Settings


def construct_base_urls(settings: Settings) -> dict:
    """Constructs a dictionary of base URLs for QRadar consoles."""
    return {"console_1": f"https://{settings.console_1_ip}"}


# TODO: Add Logic to split query if the time range is too large or add logic to create a time_range based on the
#  query name.


def adjust_stop_time(search_params):
    """ """
    large_queries = []
    medium_queries = []
    start_time = datetime.strptime(search_params["start_time"], "%Y-%m-%d %H:%M:%S")
    stop_time = datetime.strptime(search_params["stop_time"], "%Y-%m-%d %H:%M:%S")
    # Calculate time difference
    time_diff = stop_time - start_time
    query = search_params["query"]
    # Check if query is in allowed list and time difference exceeds 3 hours
    if query["query_name"] in large_queries and time_diff > timedelta(hours=3):
        logger.debug(
            "Time difference is greater than 3 hours for a larger query. Creating chunks of 3 hours."
        )
        new_stop_time = start_time + timedelta(hours=3)
        split_query = True
        return search_params["start_time"], new_stop_time, split_query
    elif query["query_name"] in medium_queries and time_diff > timedelta(hours=6):
        logger.debug(
            "Time difference is greater than 6 hours for a medium query. Creating chunks of 6 hours."
        )
        new_stop_time = start_time + timedelta(hours=6)
        split_query = True
        return search_params["start_time"], new_stop_time, split_query
    else:
        split_query = False
        return search_params["start_time"], stop_time, split_query


# TODO: Unpack the query dictionary into query_name and query_expression.
def get_search_params(event_processor: int, customer_name: str, query: dict):
    """ """
    search_params = {
        "event_processor": event_processor,
        "customer_name": customer_name,
        "query": {
            "query_name": query["query_name"],
            "query_expression": query["query_expression"],
        },
        "start_time": "2024-07-27 00:00:00",
        "stop_time": "2024-08-03 00:00:00",
    }
    try:
        start_time, new_stop_time, split_query = adjust_stop_time(search_params)
        search_params["stop_time"] = new_stop_time.strftime("%Y-%m-%d %H:%M:%S")
        search_params["query"]["query_expression"] = search_params["query"][
            "query_expression"
        ].format(
            customer_name=search_params["customer_name"],
            start_time=start_time,
            stop_time=new_stop_time.strftime("%Y-%m-%d %H:%M:%S"),
            event_processor=search_params["event_processor"],
        )
        return search_params, split_query
    except KeyError:
        raise
    except Exception:
        raise
