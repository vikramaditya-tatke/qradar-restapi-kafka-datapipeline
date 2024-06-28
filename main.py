from qradarconnector import QRadarConnector
from settings import settings
from pipeline_logger import logger
from producer import create_producer


def construct_base_urls(
    console_1_ip: str,
    console_2_ip: str,
    console_3_ip: str,
    console_aa_ip: str,
    console_aus_ip: str,
    console_uae_ip: str,
    console_us_ip: str,
) -> dict:
    base_urls = {
        console_1_ip: f"https://{console_1_ip}",
        console_2_ip: f"https://{console_2_ip}",
        console_3_ip: f"https://{console_3_ip}",
        console_aa_ip: f"https://{console_aa_ip}",
        console_aus_ip: f"https://{console_aus_ip}",
        console_uae_ip: f"https://{console_uae_ip}",
        console_us_ip: f"https://{console_us_ip}",
    }
    return base_urls


if __name__ == "__main__":
    logger.debug("Application Started")
    base_urls = construct_base_urls(
        settings.console_1_ip,
        settings.console_2_ip,
        settings.console_3_ip,
        settings.console_aa_ip,
        settings.console_aus_ip,
        settings.console_uae_ip,
        settings.console_us_ip,
    )
    logger.debug("Generated base_urls")
    producer = create_producer()
    logger.debug("Created Kafka Producer")
    qradar_connector = QRadarConnector(
        sec_token=settings.console_1_token,
        base_url=base_urls.get(settings.console_1_ip),
    )
    logger.debug("Created QRadarConnector object")
    search = qradar_connector.trigger_search()
    logger.info("Search Triggered")
    attempt = 1
    polling = None
    search_completed = None
    logger.debug("Started Polling")
    while attempt <= settings.max_attempts:
        polling = qradar_connector.get_search_status(search["cursor_id"])
        logger.info(f"Polling using Ariel Cursor ID")
        if polling:
            search_completed == polling.get["completed"]
            logger.info(f"Search Completed")
    if search_completed:
        logger.info("Started Data Fetching")
        qradar_connector.receive_data(polling)
    else:
        logger.warning("Search timed out")
