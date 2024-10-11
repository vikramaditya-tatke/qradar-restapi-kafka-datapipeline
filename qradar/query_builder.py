from datetime import datetime, timedelta

from pipeline_logger import logger


def get_query_size(query_name: str) -> str:
    """Determine the size of the query based on the query name."""
    large_queries = {
        # "AllowedOutboundTraffic",
        # "AllowedInboundTraffic",
        # "AWS_TopEventName",
    }
    medium_queries = {
        # "AuthenticationFailure",
        # "AuthenticationSuccess",
        # "TopSecurityEvents",
    }

    if query_name in large_queries:
        return "large"
    elif query_name in medium_queries:
        return "medium"
    else:
        return "small"


def split_time_range(
    start_time: datetime, stop_time: datetime, chunk_size: timedelta
) -> list:
    """
    Splits the time range into chunks of the given size.
    Returns a list of (start_time, stop_time) tuples.
    """
    times = []
    current_start = start_time

    while current_start < stop_time:
        current_stop = min(current_start + chunk_size, stop_time)
        times.append((current_start, current_stop))
        current_start = current_stop

    return times


def unpack_query_details(query: dict) -> (str, str):
    """Extracts and validates query_name and query_expression from the query dictionary."""
    query_name = query.get("query_name")
    query_expression = query.get("query_expression")
    if not query_name or not query_expression:
        raise KeyError("query_name or query_expression missing.")
    return query_name, query_expression


def parse_duration(duration: dict) -> (datetime, datetime):
    """Parses and validates start_time and stop_time from the duration dictionary."""
    start_time_str = duration.get("start_time")
    stop_time_str = duration.get("stop_time")
    if not start_time_str or not stop_time_str:
        raise KeyError("start_time or stop_time missing in duration dictionary.")
    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    stop_time = datetime.strptime(stop_time_str, "%Y-%m-%d %H:%M:%S")
    return start_time, stop_time


def format_query_expression(
    query_expression: str,
    customer_name: str,
    start_time: str,
    stop_time: str,
    event_processor: int,
) -> str:
    """Formats the query expression with the given parameters."""
    return query_expression.format(
        customer_name=customer_name,
        start_time=start_time,
        stop_time=stop_time,
        event_processor=event_processor,
    )


def generate_search_params_list(
    customer_name, event_processor, query_expression, query_name, time_ranges
):
    # Build search_params for each time chunk
    search_params_list = []
    for idx, (chunk_start, chunk_stop) in enumerate(time_ranges):
        chunk_start_str = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
        chunk_stop_str = chunk_stop.strftime("%Y-%m-%d %H:%M:%S")

        # Format the query expression
        formatted_query_expression = format_query_expression(
            query_expression,
            customer_name,
            chunk_start_str,
            chunk_stop_str,
            event_processor,
        )

        search_params = {
            "event_processor": event_processor,
            "customer_name": customer_name,
            "query": {
                "query_name": query_name,
                "query_expression": formatted_query_expression,
            },
            "start_time": chunk_start_str,
            "stop_time": chunk_stop_str,
            "chunk_index": idx,
        }
        search_params_list.append(search_params)
    return search_params_list


def get_search_params(
    event_processor: int, customer_name: str, query: dict, duration: dict
) -> list:
    """
    Prepares search parameters by determining the query size and adjusting the time range
    if necessary. Returns a list of search_params dictionaries.
    """
    try:
        # Validate customer_name
        if not customer_name:
            raise KeyError("Customer name is missing.")

        # Unpack and validate query details
        query_name, query_expression = unpack_query_details(query)

        # Parse duration into datetime objects
        start_time, stop_time = parse_duration(duration)

        # Determine query size and chunk size
        query_size = get_query_size(query_name)
        if query_size == "large":
            chunk_size = timedelta(hours=3)
        elif query_size == "medium":
            chunk_size = timedelta(hours=6)
        else:
            chunk_size = stop_time - start_time  # No splitting for small queries

        # Split time range into chunks
        time_ranges = split_time_range(start_time, stop_time, chunk_size)

        search_params_list = generate_search_params_list(
            customer_name,
            event_processor,
            query_expression,
            query_name,
            time_ranges,
        )

        return search_params_list

    except KeyError as e:
        logger.error(f"Missing key in get_search_params: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error generating search parameters: {str(e)}")
        raise
