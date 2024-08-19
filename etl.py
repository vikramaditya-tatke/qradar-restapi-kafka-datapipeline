import asyncio
import time
from typing import Generator, Tuple, List, Dict, Any

import clickhouse_connect
import requests
from tqdm import tqdm

from clickhouse import clickhouse, helpers
from clickhouse.clickhouse import process_batch_async
# Set up a basic logger
from pipeline_logger import logger
from qradar.qradarconnector import parse_qradar_data
from settings import settings


class ETLPipeline:
    def __init__(
        self, response: requests.Response, search_params: Dict[str, Any], base_url: str
    ):
        self.response = response
        self.search_params = search_params
        self.search_params["batch_size"] = settings.clickhouse_batch_size
        self.base_url = base_url
        self.customer_name = self._sanitize_customer_name(
            search_params["customer_name"]
        )
        self.query_name = search_params["query"]["query_name"]
        self.click_house_table_name = f"{self.customer_name}_{self.query_name}"
        self.progress_bar = None

    def _initialize_progress_bar(self) -> tqdm:
        return tqdm(
            total=self.search_params["response_header"]["record_count"],
            desc=f"Receiving data for {self.search_params['customer_name']} {self.search_params['event_processor']} {self.search_params['query']['query_name']} {self.search_params['response_header']['cursor_id']}",
        )

    @staticmethod
    def _sanitize_customer_name(customer_name: str) -> str:
        return (
            customer_name.replace(" ", "")
            .replace("'", "")
            .replace('"', "")
            .replace("&", "")
            .replace("_", "")
        )

    def fetch_data(cursor_id: str, max_record_count: int) -> requests.Response:
        current_record_count = 0
        response = requests.get(
            url=f"https://192.168.168.11/api/ariel/searches/{cursor_id}/results",
            headers={
                "Range": f"items={current_record_count}-{max_record_count}",
            },
            stream=True,
            verify=False,
        )
        response.raise_for_status()
        return response

    def parse_qradar_data(
        response: requests.Response, parser_key: str
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            for event in ijson.items(response.raw, parser_key):
                transformed_event = rename_event(event)
                transformed_event = add_date(transformed_event)
                yield transformed_event
        except ValueError:
            raise
        except ijson.common.IncompleteJSONError:
            raise

    def _extract_batches(
        self,
    ) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
        batch = []
        current_record_count = 0
        self.progress_bar = self._initialize_progress_bar()
        for event in parse_qradar_data(
            self.response, self.search_params["parser_key"]
        ):  # Use the method within the class
            current_record_count += 1
            self.progress_bar.update()
            batch.append(event)

            if len(batch) >= settings.clickhouse_batch_size:
                yield batch, current_record_count
                batch = []
        if batch:
            yield batch, current_record_count

    def transform(
        self, batch: List[Dict[str, Any]]
    ) -> Tuple[Any, List[str], List[str]]:
        return helpers.transform_raw(batch, self.search_params["query"]["query_name"])

    def load(self, rows: Any) -> None:
        try:
            asyncio.run(process_batch_async(rows, self.click_house_table_name))
        except clickhouse_connect.driver.exceptions.DataError as data_err:
            logger.error(f"Data type mismatch error in ClickHouse: {data_err}")
            raise
        except clickhouse_connect.driver.exceptions.DatabaseError as db_err:
            logger.error(f"Database error in ClickHouse: {db_err}")
            raise
        except Exception as load_err:
            logger.error(f"An unexpected error occurred during loading: {load_err}")
            raise

    def run(self) -> None:
        try:
            batch_generator = self._extract_batches()
            # Create the table before processing batches
            first_batch, _ = next(batch_generator)
            rows, summing_fields, fields = self.transform(first_batch)
            clickhouse.create_summing_merge_tree_table(
                self.click_house_table_name, fields, summing_fields
            )

            # Process the first batch
            self.load(rows)

            # Process subsequent batches
            start = time.perf_counter()
            for batch, current_record_count in batch_generator:
                rows, _, _ = self.transform(batch)
                self.load(rows)
            stop = time.perf_counter()

            if self.progress_bar:
                self.progress_bar.close()

            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            qradar_log = self.search_params.pop("response_header")
            logger.info(
                "Search Results Ingested",
                extra={"ApplicationLog": self.search_params, "QRadarLog": qradar_log},
            )

        except Exception as general_err:
            logger.error(f"ETL failed: {general_err}")
            raise


def etl(
    response: requests.Response, search_params: Dict[str, Any], base_url: str
) -> None:
    try:
        pipeline = ETLPipeline(response, search_params, base_url)
        pipeline.run()
    except Exception as err:
        print(err)


# @retry(
#     stop=stop_after_attempt(settings.max_attempts),
#     wait=wait_exponential(multiplier=1, min=4, max=10),
#     retry_error_callback=lambda retry_state: logger.error(
#         f"Retry attempt {retry_state.attempt_number} failed."
#     ),
#     reraise=True,  # Reraise the final exception
# )
# def etl_with_retry(
#     session: requests.Session, search_params: Dict[str, Any], base_url: str
# ) -> None: ...


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
#         rows
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
