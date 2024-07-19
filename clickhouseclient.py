import ipaddress
from datetime import datetime
from typing import List, Dict

import clickhouse_connect
import pyarrow as pa

try:
    client = clickhouse_connect.get_client(
        host="dazztr9s8r.eu-west-2.aws.clickhouse.cloud",
        user="default",
        password="n~I45WVrpm.HT",
        port=443,
        secure=True,
        compress="zstd",
        settings={
            "insert_deduplicate": False,
        },
    )
except Exception as e:
    print(f"Failed to connect to ClickHouse: {e}")


def clean_column_name(name):
    """Removes whitespaces from column names."""
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
    )


def push_data_to_clickhouse(data: List[Dict], customer_name, query_name):
    """Inserts a list of dictionaries into ClickHouse using PyArrow."""
    try:
        # Convert the data to a PyArrow Table
        cleaned_data = [{clean_column_name(k): v for k, v in d.items()} for d in data]
        array_data = {
            key: [convert_value(key, d[key]) for d in cleaned_data]
            for key in cleaned_data[0].keys()
        }

        data_table = pa.table(array_data)
        click_house_table_name = f"{customer_name}_{query_name}"
        # Create table if it doesn't exist
        schema = data_table.schema
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {click_house_table_name} (
                {", ".join([f"{field.name} {get_clickhouse_type(field.name, field.type)}" for field in schema])}
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(Start_Time)
            ORDER BY tuple()
        """
        client.command(create_table_query)

        # Insert data using Arrow table
        client.insert_arrow(click_house_table_name, data_table)
    except Exception as ee:
        print(f"Failed to insert list of dictionaries into ClickHouse: {ee}")
        raise


def convert_value(field_name, value):
    """Converts values to appropriate types for Arrow table."""
    if is_ipv4_address(value):
        return str(value)
    elif field_name == "Start_Time":
        # Convert epoch milliseconds to datetime
        return pa.scalar(value, type=pa.timestamp("ms"))
    elif isinstance(value, datetime):
        return value.isoformat()
    return value


def is_ipv4_address(value):
    """Checks if the value is a valid IPv4 address."""
    try:
        ipaddress.IPv4Address(value)
        return True
    except ValueError:
        return False


def get_clickhouse_type(field_name, dtype):
    """Infers ClickHouse data type from a PyArrow dtype and field name."""
    if pa.types.is_int64(dtype):
        return "Nullable(Int64)"
    elif pa.types.is_float64(dtype):
        return "Nullable(Float64)"
    elif pa.types.is_decimal128(dtype):
        return "Nullable(Decimal)"
    elif pa.types.is_string(dtype):
        if field_name in ["Source_IP", "Destination_IP"]:
            return "Nullable(IPv4)"
        if field_name == "Event_Count":
            return "Int64"
        return "Nullable(String)"
    elif pa.types.is_timestamp(dtype):
        return "DateTime64(3)"
    elif pa.types.is_boolean(dtype):
        return "Nullable(UInt8)"
    elif pa.types.is_binary(dtype):
        return "Nullable(String)"
    elif pa.types.is_null(dtype):
        return "Nullable(String)"
    else:
        raise ValueError(f"Unsupported data type: {dtype}")
