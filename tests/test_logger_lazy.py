import sys
import unittest
from unittest.mock import MagicMock

# Mock dependencies to avoid side effects and environment requirements
mock_config = MagicMock()
mock_settings = MagicMock()
# Setup mock settings attributes required by logger
mock_settings.clickhouse_base_url = "http://localhost"
mock_settings.clickhouse_port = 8123
mock_settings.clickhouse_user = "default"
mock_settings.clickhouse_password = ""
mock_settings.clickhouse_compression_protocol = "lz4"
mock_settings.default_timeout = 30

mock_config.settings = mock_settings
sys.modules["src.utils.config"] = mock_config

mock_clickhouse = MagicMock()
sys.modules["clickhouse_connect"] = mock_clickhouse
sys.modules["orjson"] = MagicMock()

# Now import the module under test
from src.utils.logger import ClickHouseHandler, ClickHousecloudHandler


class TestLazyLogger(unittest.TestCase):
    def setUp(self):
        mock_clickhouse.get_client.reset_mock()

    def test_clickhouse_handler_lazy_init(self):
        """Verify ClickHouseHandler does not connect on init"""
        handler = ClickHouseHandler()
        mock_clickhouse.get_client.assert_not_called()

        # Verify connect calls get_client
        handler.connect()
        mock_clickhouse.get_client.assert_called_once()

    def test_clickhouse_cloud_handler_lazy_init(self):
        """Verify ClickHousecloudHandler does not connect on init"""
        handler = ClickHousecloudHandler()
        mock_clickhouse.get_client.assert_not_called()

        # Verify connect calls get_client
        handler.connect()
        mock_clickhouse.get_client.assert_called_once()


if __name__ == "__main__":
    unittest.main()
