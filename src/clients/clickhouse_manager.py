import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError

from src.utils.config import settings
from src.utils.logger import logger


class ClickHouseClientManager:
    @classmethod
    async def get_client(cls) -> AsyncClient:
        """Creates and returns a new async client instance."""
        try:
            client = await clickhouse_connect.get_async_client(
                host=settings.clickhouse_base_url,
                port=settings.clickhouse_port,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
                secure=settings.clickhouse_secure,
                compress=settings.clickhouse_compression_protocol,
                connect_timeout=settings.default_timeout,
                send_receive_timeout=settings.default_timeout,
                settings={
                    "insert_deduplicate": True,
                },
            )
            logger.info("Initialized new ClickHouse AsyncClient")
            return client
        except DatabaseError as e:
            logger.critical(f"Failed to connect to ClickHouse: {e}")
            raise
        except Exception as e:
            logger.critical(f"Unexpected error connecting to ClickHouse: {e}")
            raise


# Global instance
clickhouse_manager = ClickHouseClientManager()
