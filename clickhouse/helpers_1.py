import polars as pl
import yaml
from typing import List, Dict, Any, Tuple
from functools import lru_cache
from datetime import datetime


@lru_cache(maxsize=1)
def load_mapping(file_path: str) -> dict:
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def get_clickhouse_type_for_dict(value: str) -> str:
    if value in ["Source IP", "Destination IP"]:
        return "IPv4"
    elif value in ["Event Count", "Bytes Sent", "Bytes Received", "QID"]:
        return "UInt64"
    elif value in ["Source Port", "Destination Port"]:
        return "UInt16"
    elif value in ["Magnitude"]:
        return "UInt8"
    elif value in ["Start Time"]:
        return "DateTime64(3)"
    elif value in ["ReportDate", "WeekFrom"]:
        return "Date"
    elif value in [
        "Username",
        "Error Code",
        "Threat Name",
        "Source Hostname",
        "Event ID",
        "Logon Type",
        "Source Workstation",
        "Process Name",
        "Policy Name",
        "Category Description",
        "URL domain",
        "bad_key",
        "Sender",
        "Recipient",
        "Command",
        "Affected Workload",
        "Application Name",
        "Account Security ID",
        "Rule Name",
        "URL Domain",
        "Action",
    ]:
        return "Nullable(String)"
    else:
        return "LowCardinality(String)"


def transform(
    batch: List[Dict[str, Any]], query_name: str, mapping_file: str
) -> Tuple[List[List[Any]], List[str], List[str]]:
    mapping_data = load_mapping(mapping_file)
    field_mapping = mapping_data.get("field_mapping", {})
    custom_order_mapping = mapping_data.get("custom_order", {})

    df = pl.DataFrame(batch)
    renamed_cols = {col: field_mapping.get(col, col) for col in df.columns}
    df = df.rename(renamed_cols)

    df = df.with_columns(
        [
            pl.when(pl.col("Start Time") > 1e10)
            .then(pl.col("Start Time") / 1000)
            .otherwise(pl.col("Start Time"))
            .alias("Start Time"),
            pl.col("Start Time").cast(pl.Datetime("ms")),
            pl.col("Start Time").dt.date().alias("ReportDate"),
            pl.col("Start Time").dt.truncate("1w", "1d").alias("WeekFrom"),
            pl.lit(datetime.utcnow()).alias("createdAt"),
        ]
    )

    custom_order = custom_order_mapping.get(query_name, df.columns)

    summing_fields = [
        f'toStartOfHour("{col}")' if col == "Start Time" else f'"{col}"'
        for col in custom_order
        if col in df.columns and col not in ["Event Count", "Score"]
    ] + [
        f'"{col}"'
        for col in df.columns
        if col not in custom_order and col not in ["Event Count", "Score"]
    ]

    rows = df.to_numpy().tolist()
    fields = [f'"{col}" {get_clickhouse_type_for_dict(col)}' for col in df.columns]

    return rows, summing_fields, fields


def read_yaml_mapping(mapping_file: str) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    mapping_data = load_mapping(mapping_file)
    return mapping_data.get("field_mapping", {}), mapping_data.get("custom_order", {})


def validate_batch(batch: List[Dict[str, Any]], required_columns: List[str]) -> bool:
    return bool(batch) and all(col in batch[0] for col in required_columns)


def log_missing_columns(missing_columns: List[str], query_name: str) -> None:
    if missing_columns:
        print(
            f"Warning: The following columns are missing for query '{query_name}': {missing_columns}"
        )
