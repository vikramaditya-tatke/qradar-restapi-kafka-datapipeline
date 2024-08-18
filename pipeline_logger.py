import sys

import loguru
import ujson
from pymongo import MongoClient


class MongoDBHandler:
    def __init__(
        self,
        mongo_uri="mongodb://Vikram:M0ng0%40DBR%23%23t!@192.168.252.130:23456/?authMechanism=DEFAULT",
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


def modify_logger():
    logger = loguru.logger.patch(patching)
    logger.remove(0)
    logger.add(
        "./logs/app.log",
        format="{extra[serialized]}",
        rotation="500 MB",
        retention="7 days",
        compression="zip",
        enqueue=True,
        catch=True,
    )

    logger.add(
        "./logs/error.log",
        level="ERROR",
        format="{extra[serialized]}",
        rotation="1 day",
        retention="7 days",
        enqueue=True,
    )

    logger.add(
        sys.stdout,
        level="DEBUG",
        enqueue=True,
        colorize=True,
    )

    # Add MongoDB handler
    mongo_handler = (
        MongoDBHandler()
    )  # You can customize the URI, db_name, and collection_name here
    logger.add(mongo_handler.emit, format="{extra[serialized]}", enqueue=True)
    return logger


logger = modify_logger()
