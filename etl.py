import asyncio
import time
from typing import Generator, Tuple, List, Dict, Any

import clickhouse_connect
import requests
from clickhouse_connect.driver.exceptions import DatabaseError

from clickhouse import helpers
from clickhouse.clickhouse import process_batch_async

# Set up a basic logger
from pipeline_logger import logger
from qradar.qradarconnector import parse_qradar_data
from settings import settings


def transform(
    batch: List[Dict[str, Any]],
) -> tuple[list[tuple[Any | None, ...]], list[str]]:
    return helpers.transform_raw(batch)


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
        self.written_rows = 0
        self.progress_bar = None

    @staticmethod
    def _sanitize_customer_name(customer_name: str) -> str:
        return (
            customer_name.replace(" ", "")
            .replace("'", "")
            .replace('"', "")
            .replace("&", "")
            # .replace("_", "")
            .replace(".", "")
        )

    def extract_batches(
        self,
    ) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
        batch = []
        current_record_count = 0
        for event in parse_qradar_data(
            self.response, self.search_params["parser_key"]
        ):  # Use the method within the class
            current_record_count += 1
            # self.progress_bar.update()
            try:
                event = helpers.add_date(event, self.qradar_log, self.search_params)
                if len(batch) >= settings.clickhouse_batch_size:
                    yield batch, current_record_count
                    batch = []
            except ValueError as ve:
                continue
            batch.append(event)
        if batch:
            yield batch, current_record_count

    def transform_first(
        self, batch: List[Dict[str, Any]]
    ) -> tuple[list[tuple[Any | None, ...]], list[str], list[str], list[Any]]:
        return helpers.transform_first_raw(
            batch, self.search_params["query"]["query_name"]
        )

    def load(self, rows: Any, column_names) -> None:
        try:
            written_rows = asyncio.run(
                process_batch_async(
                    rows=rows,
                    column_names=column_names,
                    click_house_table_name=self.click_house_table_name,
                )
            )
            if written_rows:
                self.written_rows += written_rows

        except clickhouse_connect.driver.exceptions.DataError as data_err:
            logger.error(f"Data type mismatch error in ClickHouse: {data_err}")
            raise
        except clickhouse_connect.driver.exceptions.DatabaseError as db_err:
            logger.error(f"Database error in ClickHouse: {db_err}")
            raise
        except Exception as load_err:
            logger.error(f"An unexpected error occurred during loading: {load_err}")
            raise

    def run_first(
        self, batch_generator: Generator[Tuple[List[Dict[str, Any]], int], None, None]
    ):
        """Runs the ETL pipeline by processing the first batch."""
        try:
            # Create the table before processing batches
            first_batch, _ = next(batch_generator)
            rows, summing_fields, fields, column_names = self.transform_first(
                first_batch
            )
            # Process the first batch
            start = time.perf_counter()
            self.load(rows, column_names)
            stop = time.perf_counter()
            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            return self.written_rows
        except KeyError as ke:
            raise

        except DatabaseError as db_err:
            raise

        except Exception as general_err:
            raise

    def run(
        self, batch_generator: Generator[Tuple[List[Dict[str, Any]], int], None, None]
    ):
        """Runs the ETL pipeline by processing subsequent batches."""
        try:
            # Process subsequent batches
            start = time.perf_counter()
            for batch, current_record_count in batch_generator:
                rows, fields = transform(batch)
                self.load(rows, fields)
            stop = time.perf_counter()

            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            return self.written_rows
        except ValueError as ve:
            logger.error(
                "ETL failed: Missing Field",
                extra={
                    "ApplicationLog": self.search_params,
                    "QRadarLog": self.qradar_log,
                },
            )
            raise

        except KeyError as ke:
            logger.error(
                "ETL failed: Missing Field",
                extra={
                    "ApplicationLog": self.search_params,
                    "QRadarLog": self.qradar_log,
                },
            )
            raise

        except DatabaseError:
            raise

        except Exception as general_err:
            logger.error(
                "ETL failed: Unknown Error",
                extra={
                    "ApplicationLog": self.search_params,
                    "QRadarLog": self.qradar_log,
                },
            )
            raise


def etl(
    response: requests.Response, search_params: Dict[str, Any], base_url: str
) -> None:
    pipeline = ETLPipeline(response, search_params, base_url)
    try:
        # pipeline.initialize_progress_bar()
        batch_generator = pipeline.extract_batches()
        records_inserted = pipeline.run_first(batch_generator)
        search_params["records_inserted"] = records_inserted
        logger.info(
            "Initial Batch Ingested",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
        records_inserted = pipeline.run(batch_generator)
        # Clean up the progress bar
        if pipeline.progress_bar:
            pipeline.progress_bar.close()
        search_params["records_inserted"] = records_inserted
        logger.info(
            "Search Results Ingested",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
    except DatabaseError as de:
        pipeline.qradar_log["description"] = de.args[0]
        logger.error(
            "ETL process failed",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
    except Exception as e:
        logger.error(
            "Unknown Error Occurred",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
