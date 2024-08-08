import clickhouse_connect
from clickhouse_connect.driver import Client
from tenacity import retry, stop_after_attempt, wait_exponential

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


def create_summing_merge_tree_table(
    click_house_table_name, client: Client, fields, summing_fields
):
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
    client.command(create_table_query)


# @retry(
#     stop=stop_after_attempt(settings.max_attempts),
#     wait=wait_exponential(multiplier=1, min=4, max=10),
#     retry_error_callback=lambda retry_state: logger.exception(
#         f"Ingesting data into ClickHouse attempt {retry_state.attempt_number} failed."
#     ),
#     reraise=True,  # Reraise the final exception
# )
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
        print(result)
    except Exception as e:
        logger.exception(e)
