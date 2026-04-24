from clickhouse_connect.driver.exceptions import DatabaseError, DataError
from src.clients.clickhouse_manager import clickhouse_manager


async def load_rows_async_using_summing_merge_tree(
    client, click_house_table_name, column_names, rows
):
    try:
        await client.insert(
            click_house_table_name,
            data=rows,
            column_names=column_names,
        )
        return len(rows)
    except DatabaseError:
        raise
    except Exception:
        raise


async def process_batch_async(client, rows, column_names, click_house_table_name):
    try:
        written_rows = await load_rows_async_using_summing_merge_tree(
            client=client,
            click_house_table_name=click_house_table_name,
            rows=rows,
            column_names=column_names,
        )
        return written_rows
    except DataError:
        raise
    except Exception:
        raise
