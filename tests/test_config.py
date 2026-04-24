import unittest
import os
from unittest.mock import MagicMock
import sys

# Mock clickhouse_connect to prevent connection attempts on import of logger
mock_clickhouse = MagicMock()
sys.modules["clickhouse_connect"] = mock_clickhouse
sys.modules["clickhouse_connect.driver"] = MagicMock()
sys.modules["clickhouse_connect.driver.exceptions"] = MagicMock()
sys.modules["clickhouse_connect.driver.asyncclient"] = MagicMock()

from src.utils.config import Settings, ConsoleConfig


class TestConfig(unittest.TestCase):
    def test_dynamic_console_parsing(self):
        # Setup environment variables
        os.environ["CONSOLE_TEST1_IP"] = "1.2.3.4"
        os.environ["CONSOLE_TEST1_TOKEN"] = "abcdef123456"
        os.environ["CONSOLE_TEST2_IP"] = "5.6.7.8"
        os.environ["CONSOLE_TEST2_TOKEN"] = "xyz789012345"

        # Other required fields
        os.environ["MAX_ATTEMPTS"] = "3"
        os.environ["DEFAULT_TIMEOUT"] = "30"
        os.environ["MAX_SEARCH_TTC_IN_SECONDS"] = "60"
        os.environ["CLICKHOUSE_BASE_URL"] = "http://localhost"
        os.environ["CLICKHOUSE_BATCH_SIZE"] = "1000"
        os.environ["CLICKHOUSE_COMPRESSION_PROTOCOL"] = "lz4"
        os.environ["CLICKHOUSE_PASSWORD"] = "pass"
        os.environ["CLICKHOUSE_PORT"] = "8123"
        os.environ["CLICKHOUSE_DATABASE"] = "db"
        os.environ["CLICKHOUSE_USER"] = "user"
        os.environ["MAX_QUERIES_PER_EVENT_PROCESSOR"] = "5"
        os.environ["MAX_EVENT_PROCESSORS_ENGAGED"] = "2"

        print("Initializing Settings...")
        try:
            # Reload settings (simulate app startup)
            settings = Settings()
            print(f"Settings initialized. Consoles: {settings.consoles}")
        except Exception as e:
            print(f"Error initializing settings: {e}")
            raise

        # Verify
        self.assertIn("test1", settings.consoles)
        self.assertIn("test2", settings.consoles)

        self.assertEqual(settings.consoles["test1"].ip, "1.2.3.4")
        self.assertEqual(settings.consoles["test1"].token, "abcdef123456")
        self.assertEqual(settings.consoles["test2"].ip, "5.6.7.8")

        # Verify SSL settings
        self.assertFalse(settings.verify_ssl)
        self.assertFalse(settings.clickhouse_secure)

        # Cleanup
        del os.environ["CONSOLE_TEST1_IP"]
        del os.environ["CONSOLE_TEST1_TOKEN"]
        del os.environ["CONSOLE_TEST2_IP"]
        del os.environ["CONSOLE_TEST2_TOKEN"]


if __name__ == "__main__":
    unittest.main()
