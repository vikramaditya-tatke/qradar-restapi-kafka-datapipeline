import clickhouse_connect
import requests
from clickhouse_connect.driver import Client, AsyncClient
from tenacity import stop_after_attempt, wait_exponential, retry

from pipeline_logger import logger
from settings import settings


def create_clickhouse_client() -> Client:
    try:
        return clickhouse_connect.get_client(
            host=settings.clickhouse_base_url,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            port=settings.clickhouse_port,
            secure=True,
            compress=settings.clickhouse_compression_protocol,
            connect_timeout=settings.default_timeout,
            send_receive_timeout=settings.default_timeout,
            settings={
                "insert_deduplicate": False,
            },
        )
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")


async def create_async_clickhouse_client() -> AsyncClient:
    try:
        client = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_base_url,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            port=settings.clickhouse_port,
            secure=True,
            compress=settings.clickhouse_compression_protocol,
            connect_timeout=settings.default_timeout,
            send_receive_timeout=settings.default_timeout,
            settings={
                "insert_deduplicate": False,
            },
        )
        return client
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")


def create_summing_merge_tree_table(click_house_table_name, fields, summing_fields):
    client = create_clickhouse_client()
    create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {click_house_table_name} (
                {", ".join(fields)}
            ) ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMMDD((WeekFrom))
            ORDER BY tuple(
                {", ".join(summing_fields)}
            )
            SETTINGS allow_nullable_key = 1, async_insert = 1
        """
    try:
        client.command(create_table_query)
    except Exception as e:
        logger.exception("Error occurred while creating ClickHouse Table")
    finally:
        client.close()


def load_arrow_using_summing_merge_tree(click_house_table_name, client, data_table):
    # Insert data using Arrow table
    try:
        result = client.insert_arrow(click_house_table_name, data_table)
        print(result)
    except Exception as e:
        logger.exception(e)


def load_dataframe_using_summing_merge_tree(click_house_table_name, client, dataframe):
    # Insert data using dataframe
    try:
        result = client.insert_df(click_house_table_name, dataframe)
    except Exception as e:
        logger.exception(e)


def load_rows_using_summing_merge_tree(click_house_table_name, client: Client, rows):
    client.insert(click_house_table_name, rows)


async def load_rows_async_using_summing_merge_tree(click_house_table_name, rows):
    client = await create_async_clickhouse_client()
    await client.insert(click_house_table_name, rows)
    client.close()


async def process_batch_async(rows, click_house_table_name):
    await load_rows_async_using_summing_merge_tree(
        click_house_table_name,
        rows,
    )


@retry(
    stop=stop_after_attempt(settings.max_attempts),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.exception(
        f"Ingesting data into ClickHouse attempt {retry_state.attempt_number} failed."
    ),
    reraise=True,  # Reraise the final exception
)
def load_json_using_summing_merge_tree(click_house_table_name, json_data):
    response = requests.post(
        url=f"https://{settings.clickhouse_base_url}:443",
        auth=(settings.clickhouse_user, settings.clickhouse_password),
        data=json_data,
        headers={"Content-Type": "application/json"},
        params={"query": f"INSERT INTO {click_house_table_name} FORMAT JSONEachRow"},
    )
    response.raise_for_status()
