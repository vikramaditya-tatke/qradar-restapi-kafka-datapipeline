# import sys
# import loguru
# import ujson
# import logging
# from logstash_async.handler import AsynchronousLogstashHandler


# def serialize(record) -> str:
#     """Serializes the log records and sets default contextual fields to be included in each log.

#     Args:
#         record (Any): Log Record.

#     Returns:
#         str: JSON String of the log record.
#     """
#     subset = {
#         "timestamp": str(record["time"]),
#         "message": record["message"],
#         "level": record["level"].name,
#         "records_inserted": record["extra"].get("records_inserted"),
#         "records_found": record["extra"].get("records_found"),
#         "response_header": record["extra"].get("response_header"),
#         "domain_name": record["extra"].get("domain_name"),
#         "event_processor": record["extra"].get("event_processor"),
#         "search_id": record["extra"].get("search_id"),
#         "search_duration_start": record["extra"].get("search_duration_start"),
#         "search_duration_stop": record["extra"].get("search_duration_stop"),
#     }
#     return ujson.dumps(subset)


# def patching(record):
#     """Adds the serialized record to the serialized key inside the extra key of the log record.

#     Args:
#         record (Any): Log Record.
#     """
#     record["extra"]["serialized"] = serialize(record)


# def modify_logger():
#     """Defines handles for the logger.

#     Returns:
#         logger: logger object of the Loguru library.
#     """
#     logger = loguru.logger.patch(patching)
#     logger.remove(0)
#     logger.add(
#         "app.log",
#         format="{extra[serialized]}",
#         rotation="1 day",
#         retention="1 minute",
#         enqueue="True",
#     )

#     logger.add(
#         sys.stdout,
#         level="DEBUG",
#         enqueue=True,
#         colorize=True,
#     )

#     host = "localhost"
#     port = 5000
#     # Get you a test logger

#     async_handler = AsynchronousLogstashHandler(host, port, database_path=None)

#     logger.add(async_handler)
#     return logger


import logging
from logstash_async.handler import AsynchronousLogstashHandler

host = "localhost"
port = 5044
# Get you a test logger
test_logger = logging.getLogger("python-logstash-logger")
# Set it to whatever level you want - default will be info
test_logger.setLevel(logging.DEBUG)
# Create a handler for it
async_handler = AsynchronousLogstashHandler(host, port, database_path=None)
# Add the handler to the logger
test_logger.addHandler(async_handler)
test_logger.info("This is an info message")

import time

while True:
    test_logger.info("This is an info message at %s", time.time())
    time.sleep(0.5)
# logger = modify_logger()

# if __name__ == "__main__":
#     logger.info("This is an info message")
#     logger.error("This is an error message")
