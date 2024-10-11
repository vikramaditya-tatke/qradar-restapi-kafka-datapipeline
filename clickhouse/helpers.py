# helpers.py

import logging
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Tuple

import polars as pl
from dateutil.relativedelta import SA, relativedelta

from settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_clickhouse_type_for_dict(key: str) -> str:
    """
    Maps event keys to their corresponding ClickHouse data types.

    Args:
        key (str): The event key.

    Returns:
        str: The ClickHouse data type.
    """
    mapping = {
        "domainName": "LowCardinality(String)",
        "Domain": "UInt16",
        "Event Name": "LowCardinality(String)",
        "Source Network": "LowCardinality(String)",
        "Log Source": "LowCardinality(String)",
        "Low Level Category": "LowCardinality(String)",
        "Destination Network": "LowCardinality(String)",
        "Source IP": "IPv4",
        "Source Port": "UInt16",
        "Destination IP": "IPv4",
        "Log Source Type": "LowCardinality(String)",
        "Destination Port": "UInt16",
        "Event Count": "UInt64",
        "Start Time": "DateTime64(3)",
        "Destination Geographic Country/Region": "LowCardinality(String)",
        "Username": "LowCardinality(String)",
        "ReportDate": "Date",
        "WeekFrom": "Date",
        # Add other mappings as necessary
    }
    return mapping.get(key, "LowCardinality(String)")


def rename_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cleans and renames event keys based on a predefined mapping.

    Args:
        event (Dict[str, Any]): The original event data.

    Returns:
        Dict[str, Any]: The renamed event data.
    """
    mapping = {
        "DomainName(DomainID)": "domainName",
        "domainId": "Domain",
        "DomainAwareFullNetworkName(SourceIP, DomainID)": "Source Network",
        "DomainAwareFullNetworkName(DestinationIP, DomainID)": "Destination Network",
        "DomainAwareFullNetworkName(SourceIP)": "Source Network",
        "DateFormatFunction(StartTime, dd/MM/yyyy)": "ReportDate",
        "SensorDeviceName(DeviceId)": "Log Source",
        "QidName(Qid)": "Event Name",
        "destinationIP": "Destination IP",
        "sourceIP": "Source IP",
        "categoryname": "Low Level Category",
        "eventcount": "Event Count",
        "sourceip": "Source IP",
        "starttime": "Start Time",
        "startTime": "Start Time",
        "Time": "Start Time",
        "qid": "QID",
        "SUM_eventCount": "Event Count",
        "CategoryName(Category)": "Low Level Category",
        "CategoryName(HighLevelCategory)": "High Level Category",
        "SensorDeviceTypeName(DeviceType)": "Log Source Type",
        "deviceType": "Log Source Type",
        "logsourceid": "Log Source",
        "userName": "Username",
        "username": "Username",
        "magnitude": "Magnitude",
        "Authentication Package": "Authentication Package",
        "qidEventId": "Event ID",
        "Logon Type": "Logon Type",
        "Logon ID": "Logon ID",
        "Rule Name (custom)": "Rule Name",
        "Impersonation Level": "Impersonation Level",
        "Source Workstation": "Source Workstation",
        "Process Name": "Process Name",
        "destinationGeographicLocation": "Destination Geographic Country/Region",
        "sourceGeographicLocation": "Source Geographic Country/Region",
        "destinationPort": "Destination Port",
        "sourcePort": "Source Port",
        "CustomProperty~null": "bad_key",
    }

    renamed_event = {mapping.get(k, k): v for k, v in event.items()}
    return renamed_event


def fill_nulls_based_on_type(
    df: pl.DataFrame, type_mapping: Dict[str, str]
) -> pl.DataFrame:
    """
    Replaces null values in the DataFrame based on the ClickHouse data types.

    Args:
        df (pl.DataFrame): The Polars DataFrame to process.
        type_mapping (Dict[str, str]): A mapping of column names to ClickHouse data types.

    Returns:
        pl.DataFrame: The DataFrame with nulls filled appropriately.
    """
    fill_expressions = []
    for col in df.columns:
        ch_type = type_mapping.get(col, "LowCardinality(String)")
        if ch_type.startswith("LowCardinality(String)") or ch_type.startswith("String"):
            fill_value = "N/A"
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        elif ch_type.startswith(("UInt", "Int")):
            fill_value = 0
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        elif ch_type.startswith("Decimal"):
            fill_value = Decimal("0.00")
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        elif ch_type.startswith("IPv4"):
            fill_value = "0.0.0.0"
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        elif ch_type.startswith("DateTime"):
            fill_value = datetime(1970, 1, 1)
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        elif ch_type.startswith("Date"):
            fill_value = date(1970, 1, 1)
            fill_expressions.append(pl.col(col).fill_null(fill_value))
        else:
            # Default to "N/A" for any unspecified types
            fill_value = "N/A"
            fill_expressions.append(pl.col(col).fill_null(fill_value))
    return df.with_columns(fill_expressions)


def replace_start_time(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replaces 'Start Time' values that are 0 with the 'Start Time' from the next row.

    Args:
        df (pl.DataFrame): The Polars DataFrame to process.

    Returns:
        pl.DataFrame: The DataFrame with 'Start Time' values corrected.
    """
    # Replace 'Start Time' == 0 with the next row's 'Start Time'
    df = df.with_columns(
        [
            pl.when(pl.col("Start Time") == 0)
            .then(pl.col("Start Time").shift(-1))
            .otherwise(pl.col("Start Time"))
            .alias("Start Time")
        ]
    )

    # For any remaining 'Start Time' == 0 or None (e.g., last row), set to a default valid value
    # Here, we choose to set it to the epoch start time (0 milliseconds)
    df = df.with_columns(
        [
            pl.when((pl.col("Start Time") == 0) | pl.col("Start Time").is_null())
            .then(pl.lit(0))  # Epoch start time in milliseconds
            .otherwise(pl.col("Start Time"))
            .alias("Start Time")
        ]
    )

    return df


def validate_dates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Validates that date columns fall within ClickHouse's acceptable range.
    Replaces invalid dates with a default value if necessary.

    Args:
        df (pl.DataFrame): The Polars DataFrame to validate.

    Returns:
        pl.DataFrame: The validated DataFrame.
    """
    min_date = date(1970, 1, 1)
    df = df.with_columns(
        [
            pl.when(pl.col("WeekFrom") < min_date)
            .then(pl.lit(min_date))
            .otherwise(pl.col("WeekFrom"))
            .alias("WeekFrom"),
            pl.when(pl.col("ReportDate") < min_date)
            .then(pl.lit(min_date))
            .otherwise(pl.col("ReportDate"))
            .alias("ReportDate"),
        ]
    )
    return df


def transform_raw(
    data: List[Dict[str, Any]]
) -> Tuple[List[Tuple[Any, ...]], List[str]]:
    """
    Transforms raw batch data into a list of tuples and extracts column names using Polars.
    Incorporates the functionality of add_date directly into the transformation.

    Args:
        data (List[Dict[str, Any]]): The batch of data to transform.

    Returns:
        Tuple[List[Tuple[Any, ...]], List[str]]: A tuple containing the transformed rows and the list of column names.
    """
    if not data:
        logger.warning("No data provided to transform_raw.")
        return [], []

    # Rename event keys
    renamed_data = [rename_event(event) for event in data]

    # Create a Polars DataFrame from the renamed data
    df = pl.DataFrame(renamed_data, infer_schema_length=settings.clickhouse_batch_size)

    # Check if 'Start Time' exists
    if "Start Time" not in df.columns:
        raise ValueError("Missing 'Start Time' or 'Time' key in JSON data.")

    # Replace 'Start Time' == 0 with the next row's 'Start Time'
    df = replace_start_time(df)

    # Convert 'Start Time' from milliseconds to seconds if necessary
    df = df.with_columns(
        [
            pl.when(pl.col("Start Time") > 1e10)
            .then(pl.col("Start Time") / 1000)
            .otherwise(pl.col("Start Time"))
            .alias("Start_Time_Seconds")
        ]
    )

    # Calculate 'ReportDate' and 'WeekFrom' based on 'Start_Time_Seconds'
    df = df.with_columns(
        [
            pl.col("Start_Time_Seconds")
            .apply(
                lambda x: datetime.fromtimestamp(x).date() if x is not None else None,
                return_dtype=pl.Date,
            )
            .alias("ReportDate"),
            pl.col("Start_Time_Seconds")
            .apply(
                lambda x: (
                    (datetime.fromtimestamp(x) + relativedelta(weekday=SA(-1))).date()
                    if x is not None
                    else None
                ),
                return_dtype=pl.Date,
            )
            .alias("WeekFrom"),
        ]
    )

    # Validate dates
    df = validate_dates(df)

    # Define ClickHouse type mapping for all columns
    column_names = df.columns
    type_mapping = {col: get_clickhouse_type_for_dict(col) for col in column_names}

    # Replace nulls based on column types
    df = fill_nulls_based_on_type(df, type_mapping)

    # Enforce data types to match ClickHouse schema
    for col, ch_type in type_mapping.items():
        if ch_type.startswith("UInt16"):
            df = df.with_columns([pl.col(col).cast(pl.UInt16)])
        elif ch_type.startswith("UInt64"):
            df = df.with_columns([pl.col(col).cast(pl.UInt64)])
        elif ch_type.startswith("Decimal"):
            df = df.with_columns([pl.col(col).cast(pl.Decimal(10, 2))])
        elif ch_type.startswith("DateTime"):
            df = df.with_columns([pl.col(col).cast(pl.Datetime)])
        elif ch_type.startswith("Date"):
            df = df.with_columns([pl.col(col).cast(pl.Date)])
        elif ch_type.startswith("IPv4"):
            # Polars does not have a native IPv4 type, ensure it's a string in the correct format
            df = df.with_columns([pl.col(col).cast(pl.Utf8)])
        elif ch_type.startswith("LowCardinality(String)") or ch_type.startswith(
            "String"
        ):
            df = df.with_columns([pl.col(col).cast(pl.Utf8)])
        # Add more type casts as necessary

    # Drop the temporary 'Start_Time_Seconds' column
    df = df.drop("Start_Time_Seconds")

    # Extract column names again in case of any changes
    column_names = df.columns

    # Convert Polars DataFrame to list of tuples using .rows()
    rows = df.rows()

    return rows, column_names


def transform_first_raw(
    data: List[Dict[str, Any]], query_name: str
) -> Tuple[List[Tuple[Any, ...]], List[str], List[str], List[Any]]:
    """
    Transforms the first batch of data, prepares fields and summing_fields for ClickHouse table creation using Polars.
    Incorporates the functionality of add_date directly into the transformation.

    Args:
        data (List[Dict[str, Any]]): The first batch of data to transform.
        query_name (str): The name of the query to determine summing_fields.

    Returns:
        Tuple containing rows, summing_fields, fields, and column_names.
    """
    if not data:
        logger.warning("No data provided to transform_first_raw.")
        return [], [], [], []

    # Rename event keys
    renamed_data = [rename_event(event) for event in data]

    # Create a Polars DataFrame from the renamed data
    df = pl.DataFrame(renamed_data, infer_schema_length=settings.clickhouse_batch_size)

    # Check if 'Start Time' exists
    if "Start Time" not in df.columns:
        raise ValueError("Missing 'Start Time' or 'Time' key in JSON data.")

    # Replace 'Start Time' == 0 with the next row's 'Start Time'
    df = replace_start_time(df)

    # Convert 'Start Time' from milliseconds to seconds if necessary
    df = df.with_columns(
        [
            pl.when(pl.col("Start Time") > 1e10)
            .then(pl.col("Start Time") / 1000)
            .otherwise(pl.col("Start Time"))
            .alias("Start_Time_Seconds")
        ]
    )

    # Calculate 'ReportDate' and 'WeekFrom' based on 'Start_Time_Seconds'
    df = df.with_columns(
        [
            pl.col("Start_Time_Seconds")
            .apply(
                lambda x: datetime.fromtimestamp(x).date() if x is not None else None,
                return_dtype=pl.Date,
            )
            .alias("ReportDate"),
            pl.col("Start_Time_Seconds")
            .apply(
                lambda x: (
                    (datetime.fromtimestamp(x) + relativedelta(weekday=SA(-1))).date()
                    if x is not None
                    else None
                ),
                return_dtype=pl.Date,
            )
            .alias("WeekFrom"),
        ]
    )

    # Validate dates
    df = validate_dates(df)

    # Define ClickHouse type mapping for all columns
    column_names = df.columns
    type_mapping = {col: get_clickhouse_type_for_dict(col) for col in column_names}

    # Replace nulls based on column types
    df = fill_nulls_based_on_type(df, type_mapping)

    # Enforce data types to match ClickHouse schema
    for col, ch_type in type_mapping.items():
        if ch_type.startswith("UInt16"):
            df = df.with_columns([pl.col(col).cast(pl.UInt16)])
        elif ch_type.startswith("UInt64"):
            df = df.with_columns([pl.col(col).cast(pl.UInt64)])
        elif ch_type.startswith("Decimal"):
            df = df.with_columns([pl.col(col).cast(pl.Decimal(10, 2))])
        elif ch_type.startswith("DateTime"):
            df = df.with_columns([pl.col(col).cast(pl.Datetime)])
        elif ch_type.startswith("Date"):
            df = df.with_columns([pl.col(col).cast(pl.Date)])
        elif ch_type.startswith("IPv4"):
            # Polars does not have a native IPv4 type, ensure it's a string in the correct format
            df = df.with_columns([pl.col(col).cast(pl.Utf8)])
        elif ch_type.startswith("LowCardinality(String)") or ch_type.startswith(
            "String"
        ):
            df = df.with_columns([pl.col(col).cast(pl.Utf8)])
        # Add more type casts as necessary

    # Drop the temporary 'Start_Time_Seconds' column
    df = df.drop("Start_Time_Seconds")

    # Extract column names again in case of any changes
    column_names = df.columns

    # Convert Polars DataFrame to list of tuples using .rows()
    rows = df.rows()

    # Define fields for ClickHouse table creation with appropriate types
    fields = [f'"{key}" {get_clickhouse_type_for_dict(key)}' for key in column_names]

    # Define summing_fields based on query_name
    if query_name == "AllowedOutboundTraffic":
        custom_order = ["WeekFrom", "Destination IP", "Destination Port"]
    elif query_name == "AllowedInboundTraffic":
        custom_order = ["WeekFrom", "Source IP"]
    elif query_name == "TopSecurityEvents":
        custom_order = ["WeekFrom", "Low Level Category", "Event Name"]
    elif query_name in ["AuthenticationFailure", "AuthenticationSuccess", "VPNAccess"]:
        custom_order = ["WeekFrom", "Username"]
    elif query_name in ["CREEvents", "UBA"]:
        custom_order = ["WeekFrom", "Event Name"]
    else:
        custom_order = ["WeekFrom"]

    # Create the summing_fields with custom order first, then the rest
    summing_fields = [
        f'toStartOfHour("{key}")' if key == "Start Time" else f'"{key}"'
        for key in custom_order
        if key in column_names
    ] + [
        f'toStartOfHour("{key}")' if key == "Start Time" else f'"{key}"'
        for key in column_names
        if key not in custom_order and key not in ["Event Count", "Score"]
    ]

    return rows, summing_fields, fields, column_names
