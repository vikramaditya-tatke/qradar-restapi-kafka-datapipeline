import polars as pl
import yaml
from typing import List, Dict, Any, Tuple
from functools import lru_cache
from datetime import datetime
import os


@lru_cache(maxsize=1)
def load_mapping(file_path: str) -> dict:
    """Loads the YAML mapping file with caching and checks for file modification."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Mapping file '{file_path}' not found.")

    modification_time = os.path.getmtime(file_path)
    if load_mapping.cache_info().currsize > 0 and load_mapping.mod_time == modification_time:
        return load_mapping.cached_data

    with open(file_path, "r") as file:
        mapping_data = yaml.safe_load(file)
        load_mapping.mod_time = modification_time
        load_mapping.cached_data = mapping_data
        return mapping_data


def get_clickhouse_type_for_dict(value: str, type_mapping_file: str) -> str:
    """Returns the ClickHouse data type for specific columns, reading from a YAML/JSON mapping."""
    mapping_data = load_mapping(type_mapping_file)
    clickhouse_type_mapping = mapping_data.get("clickhouse_type_mapping", {})
    nullable_columns = set(mapping_data.get("nullable_columns", []))

    # Check if the field is in nullable fields
    if value in nullable_columns:
        return "Nullable(String)"

    # Return the mapped type or default to LowCardinality(String)
    return clickhouse_type_mapping.get(value, "LowCardinality(String)")


def create_mapped_dataframe(batch: List[Dict[str, Any]], field_mapping: [Dict[str, Any]]) -> pl.DataFrame:
    """Creates a Polars DataFrame from the input batch and applies field mappings."""
    df = pl.DataFrame(batch)
    renamed_cols = {col: field_mapping.get(col, col) for col in df.columns}
    df = df.rename(renamed_cols)

    # Handle time-based columns in one step for efficiency
    df = df.with_columns(
        [
            (pl.col("Start Time") / 1000).alias("Start Time") if df["Start Time"].max() > 1e10 else pl.col(
                "Start Time"),
            pl.col("Start Time").cast(pl.Datetime("ms")),
            pl.col("Start Time").dt.date().alias("ReportDate"),
            pl.col("Start Time").dt.truncate("1w", "1d").alias("WeekFrom"),
            pl.lit(datetime.utcnow()).alias("createdAt"),
        ]
    )
    return df


def extract_sort_key(df: pl.DataFrame, mapping_data: Dict[str, Any], query_name: str) -> List[str]:
    # Generate custom order or default to all columns
    custom_order_mapping = mapping_data.get("custom_order", {})
    custom_order = custom_order_mapping.get(query_name, df.columns)

    # Create sort key (excluding specific columns like "Event Count", "Score")
    sort_key = [
        f'toStartOfHour("{col}")' if col == "Start Time" else f'"{col}"'
        for col in custom_order
        if col in df.columns and col not in ["Event Count", "Score"]
        ] + [
        f'"{col}"'
        for col in df.columns
        if col not in custom_order and col not in ["Event Count", "Score"]
    ]

    return sort_key


def transform(
        batch: List[Dict[str, Any]], query_name: str, mapping_file: str, type_mapping_file: str
) -> Tuple[List[List[Any]], List[str], List[str]]:
    """Transforms the input batch into a ClickHouse-compatible format and returns the data, sort key, and columns."""
    # Load YAML mappings
    mapping_data = load_mapping(mapping_file)
    field_mapping = mapping_data.get("field_mapping", {})

    mapped_df = create_mapped_dataframe(batch, field_mapping)
    sort_key = extract_sort_key(mapped_df, mapping_data, query_name)
    columns = [f'"{col}" {get_clickhouse_type_for_dict(col, type_mapping_file)}' for col in mapped_df.columns]
    # Convert DataFrame to list of rows and ClickHouse field types
    rows = mapped_df.to_numpy().tolist()

    return rows, sort_key, columns


def read_yaml_mapping(mapping_file: str) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Reads field mappings and custom order from a YAML file."""
    mapping_data = load_mapping(mapping_file)
    return mapping_data.get("field_mapping", {}), mapping_data.get("custom_order", {})


def validate_batch(batch: List[Dict[str, Any]], required_columns: List[str]) -> bool:
    """Validates if the batch contains all required columns."""
    return bool(batch) and all(col in batch[0] for col in required_columns)


def log_missing_columns(missing_columns: List[str], query_name: str) -> None:
    """Logs a warning if any required columns are missing in the batch."""
    if missing_columns:
        print(f"Warning: The following columns are missing for query '{query_name}': {missing_columns}")
