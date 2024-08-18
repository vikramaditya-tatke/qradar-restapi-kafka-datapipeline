import ipaddress
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pandas as pd
import pyarrow as pa
from dateutil.relativedelta import SA, relativedelta

from pipeline_logger import logger


def rename_event(event):
    """Cleans and renames event keys efficiently."""
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
        "destinationip": "Destination IP",
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


def add_date(line_json):
    """
    Enhances a JSON object with date-related fields:

    - WeekFrom: The previous Saturday's date.
    - ReportDate: The date extracted from the JSON, formatted.
    - createdAt: The current UTC timestamp.

    Args: line_json: A dictionary-like JSON object containing either "Start Time" or "Time" (in milliseconds or
    seconds since the epoch).

    Returns:
        The modified JSON object.
    """

    query_date_epoch = line_json.get("Start Time") or line_json.get("Time")

    if query_date_epoch is None:
        raise ValueError("Missing 'Start Time' or 'Time' key in JSON data.")

    # Determine timestamp type (milliseconds or seconds) and adjust if needed
    if query_date_epoch > 1e10:
        query_timestamp = query_date_epoch / 1000
    else:
        query_timestamp = query_date_epoch
        line_json["Start Time"] = (
            query_date_epoch * 1000
        )  # Converting epoch to epoch milliseconds.

    base_date = datetime.fromtimestamp(query_timestamp)
    previous_saturday = base_date + relativedelta(weekday=SA(-1))
    line_json["Event Count"] = int(line_json["Event Count"])
    line_json["WeekFrom"] = previous_saturday.date()
    line_json["ReportDate"] = base_date.date()

    return line_json


# TODO: Replace these in the rename event function
def clean_column_name(field_name: str) -> str:
    """Removes special characters from column names."""
    return (
        field_name.replace(" ", "_")
        .replace("/", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
    )


def convert_value(field_name: str, value: Any) -> Any:
    """Converts values to appropriate types for Arrow table."""
    if field_name in [
        "Source_IP",
        "Destination_IP",
        "destinationip",
        "sourceip",
    ] and is_ipv4_address(value):
        return str(value)
    elif field_name == "Start_Time":
        # Convert epoch milliseconds to datetime
        if isinstance(value, (int, float)):
            if value > 1e10:
                return pa.scalar(value, type=pa.timestamp("ms"))
            else:
                return pa.scalar(value * 1000, type=pa.timestamp("ms"))
        else:
            logger.warning(f"Unexpected value type for Start_Time: {value}")
            return None
    elif isinstance(value, datetime):
        return value.isoformat()
    return value


def is_ipv4_address(value: Any) -> bool:
    """Checks if the value is a valid IPv4 address."""
    try:
        ipaddress.IPv4Address(value)
        return True
    except ValueError:
        return False


def get_clickhouse_type(field_name: str, dtype: pa.DataType) -> str:
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


def transform_to_arrow(
    data: List[Dict[str, Any]],
) -> Tuple[pa.Table, List[str], List[str]]:
    """Transforms a list of dictionaries into an Arrow table and prepares metadata for ClickHouse."""
    try:
        # Clean column names
        cleaned_data = [{clean_column_name(k): v for k, v in d.items()} for d in data]

        # Convert values to appropriate types
        array_data = {
            key: [convert_value(key, d[key]) for d in cleaned_data]
            for key in cleaned_data[0].keys()
        }

        # Convert to PyArrow Table
        data_table = pa.table(array_data)
        schema = data_table.schema

        # Prepare ClickHouse field definitions
        fields = [
            f"{field.name} {get_clickhouse_type(field.name, field.type)}"
            for field in schema
        ]

        # Prepare summing fields for ClickHouse if required
        summing_fields = [
            (
                f"toStartOfHour({field.name})"
                if field.name == "Start_Time"
                else field.name
            )
            for field in schema
            if field.name != "Event_Count"
        ]

        return data_table, summing_fields, fields
    except Exception as ee:
        logger.error(f"Data Transformation Failed: {ee}")
        raise


def get_clickhouse_type_for_dataframe(dtype, col_name: str = None) -> str:
    """Maps Pandas data types to ClickHouse data types."""
    type_map = {
        "object": "String",  # Pandas uses 'object' for strings
        "int64": "Int64",
        "float64": "Float64",
        "bool": "UInt8",
        "datetime64[ns]": "DateTime64(3)",  # Pandas datetime type
        "datetime64[ns, UTC]": "DateTime64(3)",  # Handle timezone-aware datetime
        "date": "Date",  # For Pandas date
        # Add more mappings as needed
    }

    # Special handling for IP addresses
    if col_name in ("ReportDate", "WeekFrom"):
        return "Date"

    return type_map.get(
        dtype.name, "String"
    )  # Default to String if not found in the map


def get_clickhouse_type_for_dict(value):
    if value in ["Source IP", "Destination IP"]:
        return "IPv4"
    elif value in ["Event Count", "Bytes Sent", "Bytes Received", "QID"]:
        return "UInt64"
    elif value in ["Source Port", "Destination Port", "Domain"]:
        return "UInt16"
    elif value in ["Domain", "Magnitude"]:
        return "UInt8"
    if value in ["Start Time"]:
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
    ]:
        return "Nullable(String)"
    else:
        return "LowCardinality(String)"


def transform_raw(
    data: List[Dict[str, Any]],
    query_name: str,
) -> tuple[list[tuple[Any, ...]], list[str], list[str]]:
    field_names = list(data[0].keys())  # Get the list of field names from the data
    rows = [tuple(row[field] for field in field_names) for row in data]  # Create rows

    # Define fields for table creation with appropriate ClickHouse types
    fields = [f'"{key}" {get_clickhouse_type_for_dict(key)}' for key in field_names]

    # Define summing fields with custom logic based on query_name
    if query_name == "AllowedOutboundTraffic":
        custom_order = ["WeekFrom", "Destination IP"]
    elif query_name == "AllowedInboundTraffic":
        custom_order = ["WeekFrom", "Source IP"]
    elif query_name == "TopSecurityEvents":
        custom_order = ["WeekFrom", "High Level Category", "Event Name"]
    elif query_name in ["AuthenticationFailure", "AuthenticationSuccess"]:
        custom_order = ["WeekFrom", "Username"]
    else:
        custom_order = ["WeekFrom"]

    # Create the summing_fields with custom order first, then the rest
    summing_fields = [
        f'toStartOfHour("{key}")' if key == "Start Time" else f'"{key}"'
        for key in custom_order
        if key in field_names
    ] + [
        f'toStartOfHour("{key}")' if key == "Start Time" else f'"{key}"'
        for key in field_names
        if key not in custom_order and key != "Event Count"
    ]

    return rows, summing_fields, fields


def transform_to_dataframe(
    data: List[Dict[str, Any]]
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Transforms data to a Pandas DataFrame and prepares metadata for ClickHouse."""
    try:
        # Create DataFrame
        df = pd.DataFrame(data)
        df["Start Time"] = pd.to_datetime(df["Start Time"], unit="ms")
        df["ReportDate"] = pd.to_datetime(df["ReportDate"], format="%d/%m/%Y").dt.date
        df["WeekFrom"] = pd.to_datetime(df["WeekFrom"], format="%d/%m/%Y").dt.date
        df.columns = df.columns.map(clean_column_name)
        # Field definitions for ClickHouse table schema
        fields = [
            (
                f'"{col}" Nullable({get_clickhouse_type_for_dataframe(dtype=df[col].dtype, col_name=col)})'
                if col == "Policy Name"
                else f'"{col}" {get_clickhouse_type_for_dataframe(dtype=df[col].dtype, col_name=col)}'
            )
            for col in df.columns
        ]

        # Summing fields for potential SummingMergeTree table (optional)
        summing_fields = [
            (f'toStartOfHour("{col}")' if col == "Start Time" else f'"{col}"')
            for col in df.columns
            if col != "Event Count"
        ]

        return df, summing_fields, fields

    except Exception as e:
        print(f"Transformation failed: {e}")
        raise
