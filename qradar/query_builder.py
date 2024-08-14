from datetime import datetime, timedelta

from pipeline_logger import logger
from settings import settings


def construct_base_urls() -> dict:
    """Constructs a dictionary of base URLs for QRadar consoles."""
    return {"console_3": f"https://{settings.console_3_ip}"}


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


def get_search_params(search_params):
    """ """
    search_params = {
        "event_processor": search_params[0],
        "customer_name": search_params[1],
        "query": {
            "query_name": search_params[2][0],
            "query_expression": search_params[2][1],
        },
        "start_time": "2024-07-27 00:00:00",
        "stop_time": "2024-07-28 00:00:00",
    }
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
