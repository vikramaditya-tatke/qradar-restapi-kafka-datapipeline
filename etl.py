import logging
from typing import Generator, Tuple, List, Dict, Any

import ijson
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm

from clickhouse import clickhouse, helpers
from settings import settings
from mykafka.producer import create_producer
import ujson

# Set up a basic logger
logger = logging.getLogger(__name__)


def _parse_qradar_data(
    response: requests.Response, parser_key: str
) -> Generator[Dict[str, Any], None, None]:
    try:
        for event in ijson.items(response.raw, parser_key):
            transformed_event = helpers.rename_event(event)
            transformed_event = helpers.add_date(transformed_event)
            yield transformed_event
    except ValueError as ve:
        logger.exception(f"Error parsing QRadar data: {ve}")


# TODO: Re-enable the progress bar.

# def initialize_progress_bar(search_params) -> tqdm:
#     return tqdm(
#         total=search_params["response_header"]["record_count"],
#         desc=f"Receiving data for {search_params['customer_name']} {search_params['query']['query_name']} {search_params['response_header']['cursor_id']} {search_params['event_processor']}",
#     )


def extract(
    response: requests.Response,
    # progress_bar: tqdm,
    parser_key: str,
) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
    batch = []
    current_record_count = 0
    for event in _parse_qradar_data(response, parser_key):
        current_record_count += 1
        # progress_bar.update()
        batch.append(event)
        if len(batch) >= settings.clickhouse_batch_size:
            yield batch, current_record_count
            batch = []
    if batch:
        yield batch, current_record_count


# TODO: Implement proper error handling
def extract_and_produce_to_kafka(
    response: requests.Response,
    # progress_bar: tqdm,
    parser_key: str,
) -> None:
    producer = create_producer()
    current_record_count = 0
    try:
        for event in _parse_qradar_data(response, parser_key):
            current_record_count += 1
            byte_event = ujson.dumps(event)
            producer.produce(topic="demo_topic", value=byte_event)
    except Exception as e:
        logger.error(f"HTTP error occurred: {e}")


def transform(batch: List[Dict[str, Any]]) -> Tuple[Any, List[str], List[str]]:
    return helpers.transform_to_arrow(batch)


# TODO: Re-enable the headers and upload to confluent cloud.


def etl(
    session: requests.Session, search_params: Dict[str, Any], base_url: str
) -> None:
    current_record_count = 0
    try:
        with session.get(
            url=f"{base_url}/api/ariel/searches/{search_params['response_header']['cursor_id']}/results",
            # headers={
            #     "SEC": settings.console_3_token,
            #     "Range": f"items={current_record_count}-{search_params['response_header']['record_count']}",
            # },
            stream=True,
            verify=False,
        ) as response:
            response.raise_for_status()
            client = clickhouse.create_clickhouse_client()
            customer_name = (
                search_params["customer_name"]
                .replace(" ", "")
                .replace("'", "")
                .replace('"', "")
                .replace("&", "")
                .replace("_", "")
            )
            query_name = search_params["query"]["query_name"]
            click_house_table_name = f"{customer_name}_{query_name}"
            # progress_bar = initialize_progress_bar(search_params)
            extract_and_produce_to_kafka(response, search_params["parser_key"])
            # for batch, current_record_count in extract(
            #     response,
            #     # progress_bar,
            #     search_params["parser_key"],
            # ):
            #     arrow_table, summing_fields, fields = transform(batch)
            #     clickhouse.create_clickhouse_table(
            #         click_house_table_name, client, fields, summing_fields
            #     )
            #     clickhouse.load_using_summing_merge_tree(
            #         click_house_table_name, client, arrow_table, summing_fields, fields
            #     )
            # progress_bar.close()
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logger.error(f"An error occurred: {err}")
        raise


# @retry(
#     stop=stop_after_attempt(settings.max_attempts),
#     wait=wait_exponential(multiplier=1, min=4, max=10),
#     retry_error_callback=lambda retry_state: logger.error(
#         f"Retry attempt {retry_state.attempt_number} failed."
#     ),
#     reraise=True,  # Re-raise the final exception
# )
def etl_with_retry(
    session: requests.Session, search_params: Dict[str, Any], base_url: str
) -> None: ...
