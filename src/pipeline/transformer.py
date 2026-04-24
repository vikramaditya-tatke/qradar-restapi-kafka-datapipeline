import asyncio
import time
from collections.abc import Generator
from typing import Any


import clickhouse_connect
import requests
from clickhouse_connect.driver.exceptions import DatabaseError

from src.pipeline import helpers
from src.clients.clickhouse import process_batch_async
from src.clients.clickhouse_manager import clickhouse_manager

# Set up a basic logger
from src.utils.logger import logger
from src.clients.qradar import parse_qradar_data
from src.utils.config import settings


def transform(
    batch: list[dict[str, Any]],
) -> tuple[list[tuple[Any | None, ...]], list[str]]:
    return helpers.transform_raw(batch)


class ETLPipeline:
    def __init__(
        self, response: requests.Response, search_params: dict[str, Any], base_url: str
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
    ) -> Generator[tuple[list[dict[str, Any]], int], None, None]:
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
            except ValueError:
                continue
            batch.append(event)
        if batch:
            yield batch, current_record_count

    def transform_first(
        self, batch: list[dict[str, Any]]
    ) -> tuple[list[tuple[Any | None, ...]], list[str], list[str], list[Any]]:
        return helpers.transform_first_raw(
            batch, self.search_params["query"]["query_name"]
        )

    async def load(self, client, rows: Any, column_names) -> None:
        try:
            written_rows = await process_batch_async(
                client=client,
                rows=rows,
                column_names=column_names,
                click_house_table_name=self.click_house_table_name,
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

    async def run_first(
        self,
        client,
        batch_generator: Generator[tuple[list[dict[str, Any]], int], None, None],
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
            await self.load(client, rows, column_names)
            stop = time.perf_counter()
            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            return self.written_rows
        except KeyError:
            raise

        except DatabaseError:
            raise

        except Exception:
            raise

    async def run(
        self,
        client,
        batch_generator: Generator[tuple[list[dict[str, Any]], int], None, None],
    ):
        """Runs the ETL pipeline by processing subsequent batches."""
        try:
            # Process subsequent batches
            start = time.perf_counter()
            for batch, current_record_count in batch_generator:
                rows, fields = transform(batch)
                await self.load(client, rows, fields)
            stop = time.perf_counter()

            self.search_params["data_ingestion_time"] = round(
                ((stop - start) / 3600), 2
            )
            return self.written_rows
        except ValueError:
            logger.error(
                "ETL failed: Missing Field",
                extra={
                    "ApplicationLog": self.search_params,
                    "QRadarLog": self.qradar_log,
                },
            )
            raise

        except KeyError:
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

        except Exception:
            logger.error(
                "ETL failed: Unknown Error",
                extra={
                    "ApplicationLog": self.search_params,
                    "QRadarLog": self.qradar_log,
                },
            )
            raise


async def etl_async(
    response: requests.Response, search_params: dict[str, Any], base_url: str
) -> None:
    pipeline = ETLPipeline(response, search_params, base_url)
    client = None
    try:
        client = await clickhouse_manager.get_client()
        # pipeline.initialize_progress_bar()
        batch_generator = pipeline.extract_batches()
        records_inserted = await pipeline.run_first(client, batch_generator)
        search_params["records_inserted"] = records_inserted
        logger.info(
            "Initial Batch Ingested",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
        records_inserted = await pipeline.run(client, batch_generator)
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
    except Exception:
        logger.error(
            "Unknown Error Occurred",
            extra={
                "ApplicationLog": search_params,
                "QRadarLog": pipeline.qradar_log,
            },
        )
    finally:
        if client:
            client.close()
            logger.info("Closed ClickHouse AsyncClient for this query")


def etl(
    response: requests.Response, search_params: dict[str, Any], base_url: str
) -> None:
    asyncio.run(etl_async(response, search_params, base_url))
