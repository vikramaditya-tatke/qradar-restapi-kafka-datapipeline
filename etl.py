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
        self.qradar_log = self.search_params.pop("response_header")
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

    def extract_batches(
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

    def run_first(self, batch_generator: Generator[Tuple[List[Dict[str, Any]], int], None, None]) -> None:
        """Runs the ETL pipeline by processing the first batch."""
        try:
            # Create the table before processing batches
            first_batch, _ = next(batch_generator)
            rows, summing_fields, fields = self.transform(first_batch)
            clickhouse.create_summing_merge_tree_table(
                self.click_house_table_name, fields, summing_fields
            )
            # Process the first batch
            self.load(rows)
            logger.info(
                "Initial Batch Ingested",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
        except KeyError as ke:
            logger.error(
                f"ETL failed: Missing Field - {ke}",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
            raise

        except Exception as general_err:
            logger.error(
                f"ETL failed: Unknown Error - {general_err}",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
            raise

    def run(self, batch_generator: Generator[Tuple[List[Dict[str, Any]], int], None, None]) -> None:
        """Runs the ETL pipeline by processing subsequent batches."""
        try:
            # Process subsequent batches
            start = time.perf_counter()
            for batch, current_record_count in batch_generator:
                rows, _, _ = self.transform(batch)
                self.load(rows)
            stop = time.perf_counter()

            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            logger.info(
                "Search Results Ingested",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
            # Clean up the progress bar
            if self.progress_bar:
                self.progress_bar.close()

        except KeyError as ke:
            logger.error(
                f"ETL failed: Missing Field - {ke}",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
            raise

        except Exception as general_err:
            logger.error(
                f"ETL failed: Unknown Error - {general_err}",
                extra={"ApplicationLog": self.search_params, "QRadarLog": self.qradar_log},
            )
            raise


def etl(
    response: requests.Response, search_params: Dict[str, Any], base_url: str
) -> None:
    try:
        pipeline = ETLPipeline(response, search_params, base_url)
        batch_generator = pipeline.extract_batches()
        pipeline.run_first(batch_generator)
        pipeline.run(batch_generator)
    except Exception as err:
        print(err)
