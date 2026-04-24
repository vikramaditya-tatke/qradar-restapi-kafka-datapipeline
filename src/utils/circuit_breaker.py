import time
from functools import wraps
from src.utils.logger import logger


class CircuitBreakerOpenException(Exception):
    """Raised when the circuit breaker is open."""

    pass


class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker opened after {self.failures} failures.")

    def record_success(self):
        if self.state == "HALF-OPEN":
            self.state = "CLOSED"
            self.failures = 0
            logger.info("Circuit breaker closed (recovered).")
        elif self.state == "CLOSED":
            self.failures = 0

    def allow_request(self):
        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF-OPEN"
                logger.info("Circuit breaker half-open, probing service.")
                return True
            return False

        if self.state == "HALF-OPEN":
            # In half-open state, we allow one request to probe.
            # If multiple threads hit this, we might want to be stricter,
            # but for now simple logic is fine.
            return True

        return True

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not self.allow_request():
                raise CircuitBreakerOpenException("Circuit breaker is open")

            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception as e:
                # We might want to filter which exceptions cause a failure
                # For now, assume all exceptions in the wrapped function are failures
                self.record_failure()
                raise e

        return wrapper

    def __enter__(self):
        if not self.allow_request():
            raise CircuitBreakerOpenException("Circuit breaker is open")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.record_failure()
            return False  # Propagate exception
        else:
            self.record_success()
            return True
