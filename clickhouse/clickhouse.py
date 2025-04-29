import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError, DataError

from pipeline_logger import logger
from settings import settings


async def create_async_clickhouse_client() -> AsyncClient:
    try:
        client = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_base_url,
            port=settings.clickhouse_port,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            secure=False,
            compress=settings.clickhouse_compression_protocol,
            connect_timeout=settings.default_timeout,
            send_receive_timeout=settings.default_timeout,
            settings={
                "insert_deduplicate": True,
            },
        )
        return client
    except DatabaseError as db_err:
        raise
    except Exception:
        raise


# TODO: Handle the clickhouse_connect.driver.exceptions.DataError


async def load_rows_async_using_summing_merge_tree(
    click_house_table_name, column_names, rows
):
    try:
        client = await create_async_clickhouse_client()
        await client.insert(
            click_house_table_name,
            data=rows,
            column_names=column_names,
        )
        client.close()
        return len(rows)
    except DatabaseError as e:
        raise
    except Exception as e:
        raise


async def process_batch_async(rows, column_names, click_house_table_name):
    try:
        written_rows = await load_rows_async_using_summing_merge_tree(
            click_house_table_name=click_house_table_name,
            rows=rows,
            column_names=column_names,
        )
        return written_rows
    except DataError as e:
        raise
    except Exception as e:
        raise
