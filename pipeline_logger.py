import sys
from datetime import datetime
from pathlib import Path
import clickhouse_connect
import loguru
import ujson
from settings import settings


class ClickHouseclouedHandler:
    def __init__(
        self,
        table="logs",
    ):
        self.client = clickhouse_connect.get_client(
            compress=settings.clickhouse_compression_protocol,
            connect_timeout=settings.default_timeout,
            send_receive_timeout=settings.default_timeout,
            settings={
                "insert_deduplicate": True,
            },
        )
        self.table = table

    def clean_float_values(self, d):
        """Recursively clean float values and convert them to integers if they are equivalent to integers."""
        if isinstance(d, dict):
            return {k: self.clean_float_values(v) for k, v in d.items()}
        elif isinstance(d, float):
            # If the float is an integer (e.g., 1.0), convert it to an integer (1)
            if d.is_integer():
                return int(d)
            return d
        return d

    def emit(self, record):
        # Check if record is already serialized, if yes, then load it
        if isinstance(record, str):
            record = ujson.loads(record)

        # Ensure the record is a dictionary
        if not isinstance(record, dict):
            raise ValueError("Log record must be a dictionary")

        log_record = record.get("extra", {}).get("serialized_dict", record)

        # Clean float values to ensure they are integers when appropriate
        log_record = self.clean_float_values(log_record)

        # Prepare the JSON object for insertion
        json_string = ujson.dumps(log_record, escape_forward_slashes=False)

        # Print the JSON string to debug
        # print(f"Prepared JSON for insertion: {json_string}")

        # Construct the insert query with the formatted JSONEachRow
        query = f"INSERT INTO {self.table} FORMAT JSONEachRow {json_string}"

        try:
            # Execute the insert using the correct query format
            self.client.command("SET input_format_import_nested_json = 1;")
            result = self.client.command(query)
        except Exception as e:
            print(f"ClickHouse Insert Error: {e}")
            # Optionally print the query or json_string if the error is related to the input
            print(f"Failed query data: {json_string}")


class ClickHouseHandler:
    def __init__(
        self,
        table="logs",
    ):
        self.client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            username="default",
            password="microsoft",
            database="DataFetchingLogs",
            compress=settings.clickhouse_compression_protocol,
            connect_timeout=settings.default_timeout,
            send_receive_timeout=settings.default_timeout,
            settings={
                "insert_deduplicate": True,
            },
        )
        self.table = table

    def emit(self, record):
        # Check if record is already serialized, if yes, then load it
        if isinstance(record, str):
            record = ujson.loads(record)

        # Ensure the record is a dictionary
        if not isinstance(record, dict):
            raise ValueError("Log record must be a dictionary")

        # Access the serialized data safely
        log_record = record.get("extra", {}).get("serialized_dict", record)

        # Define columns and values for ClickHouse insert
        columns = list(log_record.keys())
        rows = [log_record[column] for column in columns]
        json_string = ujson.dumps(log_record)
        # Construct insert query
        query = f"INSERT INTO {self.table} FORMAT JSONEachRow {json_string}"

        # Execute the insert using connection from the pool
        try:
            self.client.command("SET input_format_import_nested_json = 1;")
            result = self.client.command(query)
        except Exception as e:
            print(e)


# TODO: Fix serialization errors when exec_info is set to True
def serialize(record) -> dict:
    """Serializes the log records and merges ApplicationLog and QRadarLog into a single dictionary."""
    flattened_extra = record["extra"].get("extra", {})
    application_log = flattened_extra.get("ApplicationLog", {})
    qradar_log = flattened_extra.get("QRadarLog", {})

    if not isinstance(application_log, dict):
        application_log = {}
    if not isinstance(qradar_log, dict):
        qradar_log = {}

    merged_log = {**application_log, **qradar_log}

    time: datetime = record["time"]
    time = time.replace(tzinfo=None)
    time = time.isoformat(timespec="milliseconds")
    final_log = {
        "timestamp": time,
        "message": record["message"],
        "level": record["level"].name,
        "module": record["module"],
        "line": record["line"],
        **merged_log,
    }
    if "snapshot" in final_log:
        del final_log["snapshot"]
    if "progress_details" in final_log:
        del final_log["progress_details"]
    if "subsearch_ids" in final_log:
        del final_log["subsearch_ids"]
    if "response_header" in final_log:
        del final_log["response_header"]
    if "save_results" in final_log:
        del final_log["save_results"]
    if "size_on_disk" in final_log:
        del final_log["size_on_disk"]
    if "chunk_index" in final_log:
        del final_log["chunk_index"]
    if "compressed_data_file_count" in final_log:
        del final_log["compressed_data_file_count"]
    if "data_file_count" in final_log:
        del final_log["data_file_count"]
    if "compressed_data_total_size" in final_log:
        del final_log["compressed_data_total_size"]
    if "index_total_size" in final_log:
        del final_log["index_total_size"]
    if "index_file_count" in final_log:
        del final_log["index_file_count"]
    if "desired_retention_time_msec" in final_log:
        del final_log["desired_retention_time_msec"]
    if "query" in final_log and isinstance(final_log["query"], dict):
        final_log["query_name"] = final_log["query"].get("query_name")
        del final_log["query"]
    if "start_time" in final_log and "stop_time" in final_log:
        final_log["start_time"] = datetime.strptime(
            final_log["start_time"], "%Y-%m-%d %H:%M:%S"
        ).isoformat(timespec="milliseconds")
        final_log["stop_time"] = datetime.strptime(
            final_log["stop_time"], "%Y-%m-%d %H:%M:%S"
        ).isoformat(timespec="milliseconds")
    additional_fields = {
        "data_ingestion_time": flattened_extra.get("data_ingestion_time"),
        "batch_size": flattened_extra.get("batch_size"),
    }
    final_log.update({k: v for k, v in additional_fields.items() if v is not None})

    return final_log


def patching(record):
    try:
        serialized_dict = serialize(record)
        # Ensure the 'extra' field exists
        record.setdefault("extra", {})
        record["extra"]["serialized_dict"] = serialized_dict
        record["extra"]["serialized"] = ujson.dumps(serialized_dict)
    except Exception as e:
        logger.error(f"Failed to serialize record: {e}. Record: {record}")


def truncate(value, max_length):
    return (
        str(value)[:max_length]
        if len(str(value)) > max_length
        else str(value).ljust(max_length)
    )


def custom_format(record):
    # Parse the serialized extra data
    extra_data = ujson.loads(record["extra"].get("serialized", "{}"))

    # Extract and truncate the fields
    event_processor = truncate(extra_data.get("event_processor", "N/A"), 4)
    customer_name = truncate(extra_data.get("customer_name", "N/A"), 25)
    query_name = truncate(extra_data.get("query_name", "N/A"), 25)
    start_time = truncate(extra_data.get("start_time", "N/A"), 20)
    stop_time = truncate(extra_data.get("stop_time", "N/A"), 20)
    progress = truncate(extra_data.get("progress", "N/A"), 5)
    record_count = truncate(extra_data.get("record_count", "N/A"), 10)
    data_ingestion_time = truncate(extra_data.get("data_ingestion_time", "N/A"), 3)
    message = truncate(record["message"], 150)
    module = truncate(record["module"], 20)
    line = record["line"]
    # Format the log message
    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <5}</level> | "
        f"<level>{line: <3}</level> | "
        f"<level>{module: <5}</level> | "
        f"<yellow>{event_processor: <3}</yellow> | "
        f"<blue>{customer_name: <25}</blue> | "
        f"<cyan>{query_name: <25}</cyan> | "
        f"<magenta>{start_time: <20}</magenta> | "
        f"<magenta>{stop_time: <20}</magenta> | "
        f"<red>{progress: <5}</red> | "
        f"<green>{record_count: <5}</green> | "
        f"<yellow>{data_ingestion_time: <3}</yellow> | "
        f"<level>{message: <30}</level> |\n"
    )


def modify_logger():
    logger = loguru.logger.patch(patching)
    logger.remove(0)  # Remove the default handler

    # Ensure log directory exists
    log_dir = Path("./logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Add handler for app.log
    logger.add(
        log_dir / "app.log",
        format="{extra[serialized]}",
        rotation="500 MB",
        retention="7 days",
        compression="zip",
        enqueue=True,
        catch=True,
        backtrace=True,
        diagnose=True,
        serialize=True,
        colorize=False,
        encoding="utf-8",
        mode="a",
    )

    # Add handler for error.log
    logger.add(
        log_dir / "error.log",
        level="ERROR",
        format="{extra[serialized]}",
        rotation="1 day",
        retention="7 days",
        compression="zip",
        enqueue=True,
        catch=True,
        backtrace=True,
        diagnose=True,
        serialize=True,
        colorize=False,
        encoding="utf-8",
        mode="a",
    )

    # Add stdout handler for debugging
    logger.add(
        sys.stdout,
        level="DEBUG",
        format=custom_format,
        enqueue=True,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

    clickhouse_cloued_handler = (
        ClickHouseclouedHandler()
    )  # Customize the host, user, password, etc. if needed
    logger.add(
        clickhouse_cloued_handler.emit, format="{extra[serialized]}", enqueue=True
    )
    # return logger

    clickhouse_handler = (
        ClickHouseHandler()
    )  # Customize the host, user, password, etc. if needed
    logger.add(clickhouse_handler.emit, format="{extra[serialized]}", enqueue=True)
    return logger


logger = modify_logger()
