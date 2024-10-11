import random
import sys
import time

from loguru import logger
from tqdm import tqdm


class TqdmLoggingHandler:
    """Custom logging handler integrating tqdm with loguru for seamless log messages."""

    def __init__(self, tqdm_instance):
        self._tqdm_instance = tqdm_instance

    def write(self, message):
        # Clear the progress bar line
        self._tqdm_instance.clear(nolock=True)
        # Write the message
        sys.stdout.write(message)
        # Redraw the progress bar
        self._tqdm_instance.refresh(nolock=True)

    def flush(self):
        self._tqdm_instance.fp.flush()


def setup_logger(progress_bar):
    logger.remove()  # Remove default logger configuration
    logger.add(sys.stderr, format="{time} | <lvl>{level: <8}</lvl> | {message}")
    logger.add(
        TqdmLoggingHandler(progress_bar),
        format="{time:YYYY-MM-DD HH:mm:ss} | <lvl>{level: <8}</lvl> | {message}",
        colorize=True,
    )


def dummy_data_generator(num_records):
    """Generates dummy data."""
    for _ in range(num_records):
        time.sleep(random.uniform(0.01, 0.05))  # Simulates time delay
        yield {"data": "dummy_data"}


def simulate_etl_process(num_records):
    """Simulate an ETL process with logging and a progress bar."""
    with tqdm(total=num_records, desc="Processing records", ncols=100) as progress_bar:
        setup_logger(progress_bar)
        for record in dummy_data_generator(num_records):
            # Using structured logging as before, logging the dictionary directly.
            logger.debug(f"Processing record: {record}")
            progress_bar.update(1)
            if progress_bar.n == num_records / 2:
                logger.info("Halfway done with processing records")
    logger.info("ETL process completed successfully")


if __name__ == "__main__":
    num_records = 100
    simulate_etl_process(num_records)
