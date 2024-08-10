import asyncio
import logging
import time
from typing import Generator, Tuple, List, Dict, Any

import ijson
import requests
import ujson
from requests.exceptions import HTTPError
from tqdm import tqdm

from clickhouse import clickhouse, helpers
from clickhouse.clickhouse import process_batch_async
from mykafka.producer import create_producer
from settings import settings

# Set up a basic logger
logger = logging.getLogger(__name__)


# TODO: Fix the IncompleteJSONError at `for event in ijson.items(response.raw, parser_key)`
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
    except ijson.common.IncompleteJSONError as ij:
        logger.error(f"Error parsing QRadar data: {ij}")


# TODO: Re-enable the progress bar.


def initialize_progress_bar(search_params) -> tqdm:
    return tqdm(
        total=search_params["response_header"]["record_count"],
        desc=f"Receiving data for {search_params['customer_name']} {search_params['query']['query_name']} {search_params['response_header']['cursor_id']} {search_params['event_processor']}",
    )


def extract(
    response: requests.Response,
    progress_bar: tqdm,
    parser_key: str,
) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
    batch = []
    current_record_count = 0
    for event in _parse_qradar_data(response, parser_key):
        current_record_count += 1
        progress_bar.update()
        batch.append(event)

        if len(batch) >= settings.clickhouse_batch_size:
            yield batch, current_record_count
            batch = []
    if batch:
        yield batch, current_record_count


def extract_str_batch(
    response: requests.Response,
    progress_bar: tqdm,
    parser_key: str,
) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
    batch = []
    current_record_count = 0
    for event in _parse_qradar_data(response, parser_key):
        current_record_count += 1
        progress_bar.update()
        str_event = ujson.dumps(event)
        batch.append(str_event)

        if len(batch) >= settings.clickhouse_batch_size:
            str_batch = "\n".join(batch)
            yield batch, str_batch, current_record_count
            batch = []
    if batch:
        str_batch = "\n".join(batch)
        yield batch, str_batch, current_record_count


# TODO: Implement proper error handling
def extract_and_produce_to_kafka(
    response: requests.Response,
    progress_bar: tqdm,
    parser_key: str,
) -> None:
    producer = create_producer()
    current_record_count = 0
    try:
        for event in _parse_qradar_data(response, parser_key):
            current_record_count += 1
            progress_bar.update()
            str_event = ujson.dumps(event)
            producer.produce(topic="demo_cluster", value=str_event)
        producer.flush()
        progress_bar.close()
    except Exception as e:
        logger.error(f"HTTP error occurred: {e}")


def transform(batch: List[Dict[str, Any]]) -> Tuple[Any, List[str], List[str]]:
    # return helpers.transform_to_arrow(batch)
    # return helpers.transform_to_dataframe(batch)
    return helpers.transform_raw(batch)


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
            progress_bar = initialize_progress_bar(search_params)

            # Create the table before processing batches
            batch_generator = extract(
                response, progress_bar, search_params["parser_key"]
            )
            first_batch, _ = next(batch_generator)
            rows, summing_fields, fields = transform(first_batch)
            clickhouse.create_summing_merge_tree_table(
                click_house_table_name, fields, summing_fields
            )

            # Process the first batch
            asyncio.run(process_batch_async(rows, click_house_table_name))
            # Process subsequent batches as they are yielded
            start = time.perf_counter()
            for batch, current_record_count in batch_generator:
                rows, _, _ = transform(batch)
                asyncio.run(process_batch_async(rows, click_house_table_name))
            stop = time.perf_counter()
            print(
                f'ROWS: {search_params["response_header"]["record_count"]}, COMPRESSION: {settings.clickhouse_compression_protocol}, BATCH_SIZE: {settings.clickhouse_batch_size}'
            )
            print(
                f'Ingestion speed: {search_params["response_header"]["record_count"]/(stop - start)} rows/sec'
            )
            # Insert into Kafka
            # extract_and_produce_to_kafka(
            #     response,
            #     progress_bar,
            #     search_params["parser_key"],
            # )

            # Insert Arrow
            # for batch, current_record_count in extract(
            #     response,
            #     progress_bar,
            #     search_params["parser_key"],
            # ):
            #     arrow_table, summing_fields, fields = transform(batch)
            #     clickhouse.create_clickhouse_table(
            #         click_house_table_name, client, fields, summing_fields
            #     )
            #     clickhouse.load_arrow_using_summing_merge_tree(
            #         click_house_table_name, client, arrow_table
            #     )

            # Insert Dataframe
            # for batch, current_record_count in extract(
            #     response,
            #     progress_bar,
            #     search_params["parser_key"],
            # ):
            #     dataframe, summing_fields, fields = transform(batch)
            #     clickhouse.create_summing_merge_tree_table(
            #         click_house_table_name, fields, client, summing_fields
            #     )
            #     clickhouse.load_dataframe_using_summing_merge_tree(
            #         click_house_table_name, client, dataframe
            #     )

            # Insert rows
            # for batch, current_record_count in extract(
            #     response,
            #     progress_bar,
            #     search_params["parser_key"],
            # ):
            #     rows, summing_fields, fields = transform(batch)
            #     clickhouse.create_summing_merge_tree_table(
            #         click_house_table_name, fields, client, summing_fields
            #     )
            #     clickhouse.load_rows_using_summing_merge_tree(
            #         click_house_table_name,
            #         client,
            #         rows,
            #     )

            # Load JSON using POST
            # for batch, str_batch, current_record_count in extract_str_batch(
            #     response,
            #     progress_bar,
            #     search_params["parser_key"],
            # ):
            #     rows, summing_fields, fields = transform(batch)
            #     clickhouse.create_summing_merge_tree_table(
            #         click_house_table_name, fields, client, summing_fields
            #     )
            #     clickhouse.load_json_using_summing_merge_tree(
            #         click_house_table_name, str_batch
            #     )

            progress_bar.close()
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
#     reraise=True,  # Reraise the final exception
# )
def etl_with_retry(
    session: requests.Session, search_params: Dict[str, Any], base_url: str
) -> None: ...
