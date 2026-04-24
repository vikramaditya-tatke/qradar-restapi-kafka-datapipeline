import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os

# Mock dependencies
sys.modules["clickhouse_connect"] = MagicMock()
sys.modules["orjson"] = MagicMock()

from src.pipeline.executor import search_executor
from src.clients.qradar import QRadarConnector


class TestExecutor(unittest.TestCase):
    def setUp(self):
        self.mock_connector = MagicMock(spec=QRadarConnector)
        self.event_processor = 1
        self.customer_name = "TestCustomer"
        self.query = {"query_expression": "SELECT * FROM events"}
        self.duration = {"start_time": "now-1h", "stop_time": "now"}

    @patch("src.pipeline.executor.generate_search_params")
    @patch("src.pipeline.executor.settings")
    def test_search_executor_success(self, mock_settings, mock_generate_params):
        # Setup mocks
        mock_settings.max_attempts = 3
        mock_generate_params.return_value = [
            {"query": self.query, "duration": self.duration}
        ]

        # Mock trigger_search response
        self.mock_connector.trigger_search.return_value = {"cursor_id": "cursor_123"}

        # Mock poll_search_status responses: running, then completed
        self.mock_connector.get_search_status.side_effect = [
            {"completed": False, "progress": 50},
            {"completed": True, "progress": 100},
        ]

        # Mock get_parser_key
        self.mock_connector.get_parser_key.return_value = {"key": "value"}

        # Execute
        result = search_executor(
            self.event_processor,
            self.customer_name,
            self.query,
            self.duration,
            self.mock_connector,
        )

        # Verify
        self.assertIsNotNone(result)
        self.assertEqual(result["parser_key"], "key")
        self.mock_connector.trigger_search.assert_called_once()
        self.assertEqual(self.mock_connector.get_search_status.call_count, 2)

    @patch("src.pipeline.executor.generate_search_params")
    @patch("src.pipeline.executor.settings")
    def test_search_executor_failure_max_attempts(
        self, mock_settings, mock_generate_params
    ):
        # Setup mocks
        mock_settings.max_attempts = 2
        mock_generate_params.return_value = [
            {"query": self.query, "duration": self.duration}
        ]

        self.mock_connector.trigger_search.return_value = {"cursor_id": "cursor_123"}
        self.mock_connector.get_search_status.return_value = {"completed": False}

        # Execute
        result = search_executor(
            self.event_processor,
            self.customer_name,
            self.query,
            self.duration,
            self.mock_connector,
        )

        # Verify
        self.assertIsNone(result)
        self.assertEqual(self.mock_connector.get_search_status.call_count, 2)


if __name__ == "__main__":
    unittest.main()
