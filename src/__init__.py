"""
QRadar to ClickHouse Data Pipeline.

This package provides a comprehensive data pipeline for extracting data from QRadar
and loading it into ClickHouse. The pipeline supports parallel processing,
data transformation, and robust error handling.

Main Components:
- clients: External service connectors (QRadar, ClickHouse)
- models: Data models and type mappings
- pipeline: Core data processing logic
- utils: Shared utilities and configuration
"""

from . import clients, models, pipeline, utils

__all__ = ["clients", "models", "pipeline", "utils"]

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
