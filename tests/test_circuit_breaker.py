import unittest
from unittest.mock import MagicMock, patch
import requests
from requests.exceptions import HTTPError

# Mock dependencies
import sys

sys.modules["clickhouse_connect"] = MagicMock()
sys.modules["orjson"] = MagicMock()

from src.clients.qradar import QRadarConnector
from src.utils.circuit_breaker import CircuitBreakerOpenException


class TestCircuitBreakerIntegration(unittest.TestCase):
    def setUp(self):
        self.mock_session = MagicMock(spec=requests.Session)
        self.connector = QRadarConnector("token", self.mock_session, "http://test")
        # Lower threshold for testing
        self.connector.circuit_breaker.failure_threshold = 2
        self.connector.circuit_breaker.recovery_timeout = 1

    def test_circuit_breaker_opens_on_failures(self):
        # Simulate failures
        self.mock_session.request.side_effect = HTTPError("500 Error")

        # Fail 1
        with self.assertRaises(HTTPError):
            self.connector._make_request("GET", "http://test")

        # Fail 2 (Threshold reached)
        with self.assertRaises(HTTPError):
            self.connector._make_request("GET", "http://test")

        # Next call should raise CircuitBreakerOpenException
        with self.assertRaises(CircuitBreakerOpenException):
            self.connector._make_request("GET", "http://test")

        # Ensure no request was made for the 3rd call
        self.assertEqual(self.mock_session.request.call_count, 2)

    def test_circuit_breaker_resets_on_success(self):
        # Fail 1
        self.mock_session.request.side_effect = HTTPError("500 Error")
        with self.assertRaises(HTTPError):
            self.connector._make_request("GET", "http://test")

        # Success
        self.mock_session.request.side_effect = None
        self.mock_session.request.return_value.status_code = 200
        self.connector._make_request("GET", "http://test")

        # Fail 1 again (should be count 1, not 2)
        self.mock_session.request.side_effect = HTTPError("500 Error")
        with self.assertRaises(HTTPError):
            self.connector._make_request("GET", "http://test")

        # Should NOT be open yet
        try:
            self.connector._make_request("GET", "http://test")
        except HTTPError:
            pass  # Expected
        except CircuitBreakerOpenException:
            self.fail("Circuit breaker should not be open")


if __name__ == "__main__":
    unittest.main()
