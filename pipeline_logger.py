import sys
from pathlib import Path

import loguru
import ujson
from pymongo import MongoClient


class MongoDBHandler:
    def __init__(
        self,
        mongo_uri="mongodb://root:M0ng0%40DBR%23%23t!@172.30.170.55:23455/?authMechanism=DEFAULT",
        db_name="DataFetchingLogs",
        collection_name="logs",
    ):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def emit(self, record):
        try:
            # Check if record is already serialized, if yes, then load it
            if isinstance(record, str):
                record = ujson.loads(record)

            # Ensure the record is a dictionary
            if not isinstance(record, dict):
                raise ValueError("Log record must be a dictionary")

            # Access the serialized data safely
            log_record = record.get("extra", {}).get("serialized_dict", record)
            self.collection.insert_one(log_record)
        except Exception as e:
            logger.error(f"Failed to write log to MongoDB: {e}. Record: {record}")


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

    final_log = {
        "timestamp": str(record["time"]),
        "message": record["message"],
        "level": record["level"].name,
        "module": record["module"],
        "line": record["line"],
        **merged_log,
    }

    additional_fields = {
        "data_ingestion_time": flattened_extra.get("data_ingestion_time"),
        "batch_size": flattened_extra.get("batch_size"),
    }

    if "query" in final_log and isinstance(final_log["query"], dict):
        final_log["query_name"] = final_log["query"].get("query_name")

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

    # Add MongoDB handler
    mongo_handler = (
        MongoDBHandler()
    )  # You can customize the URI, db_name, and collection_name here
    logger.add(mongo_handler.emit, format="{extra[serialized]}", enqueue=True)
    return logger


logger = modify_logger()
