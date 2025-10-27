# QRadar to ClickHouse Data Pipeline

A robust data pipeline that extracts security event data from QRadar consoles and loads it into ClickHouse for analysis and reporting.

## Project Structure

```
qradar-clickhouse-pipeline/          
├── README.md
├── WARP.md
├── pyproject.toml
├── poetry.lock
├── .env
├── .gitignore
│
├── src/                              # Source code
│   ├── __init__.py
│   │
│   ├── clients/                      # External service clients
│   │   ├── __init__.py
│   │   ├── qradar.py                # QRadar REST API client
│   │   └── clickhouse.py            # ClickHouse client
│   │
│   ├── pipeline/                     # Data pipeline logic
│   │   ├── __init__.py
│   │   ├── executor.py              # Search execution logic
│   │   ├── transformer.py           # Data transformation
│   │   └── query_builder.py         # Query building logic
│   │
│   ├── models/                       # Data models and schemas
│   │   ├── __init__.py
│   │   ├── clickhouse_types.py      # Type mappings
│   │   └── attributes.py            # Attribute configurations
│   │
│   └── utils/                        # Shared utilities
│       ├── __init__.py
│       ├── logger.py                # Logging configuration
│       └── config.py                # Settings/configuration loader
│
├── config/                           # Configuration files
│   ├── duration.json
│   ├── ep_clients.json
│   └── queries.json
│
├── data/                             # Data files
│   └── query/                       # Query templates
│
├── logs/                             # Application logs
│   └── .gitkeep
│
└── scripts/                          # Executable scripts
    └── run.py                       # Main entry point
```

## Overview

This pipeline provides:
- **Parallel Processing**: Concurrent execution across multiple QRadar consoles and event processors
- **Robust Error Handling**: Comprehensive retry mechanisms and error logging
- **Data Transformation**: Automatic data cleaning, type conversion, and normalization
- **Flexible Querying**: Support for custom AQL queries with time-based chunking
- **Scalable Architecture**: Designed to handle large volumes of security event data

## Key Components

### Clients (`src/clients/`)
- **QRadar Connector**: Handles authentication, search execution, and data retrieval from QRadar consoles
- **ClickHouse Connector**: Manages async data insertion into ClickHouse with proper type handling

### Pipeline (`src/pipeline/`)
- **Executor**: Orchestrates search execution with retry logic and status polling
- **Transformer**: Performs ETL operations including data cleaning and type conversion
- **Query Builder**: Constructs AQL queries with proper time range management

### Models (`src/models/`)
- **Attributes**: Manages configuration loading and attribute mappings
- **ClickHouse Types**: Defines data type mappings for optimal ClickHouse storage

### Utils (`src/utils/`)
- **Logger**: Centralized logging with structured output to ClickHouse and files
- **Config**: Environment-based configuration management with validation

## Quick Start

1. **Install Dependencies**:
   ```bash
   poetry install
   ```

2. **Configure Environment**:
   Copy `.env.example` to `.env` and configure your QRadar and ClickHouse settings.

3. **Run the Pipeline**:
   ```bash
   python scripts/run.py --console <console_id> --max-threads <threads>
   ```

## Configuration

### Environment Variables
- QRadar console IPs and authentication tokens
- ClickHouse connection settings
- Batch sizes and timeout configurations

### Query Configuration
- `config/queries.json`: AQL query definitions
- `config/ep_clients.json`: Event processor to customer mappings
- `config/duration.json`: Time range settings

## Features

- **Automatic Table Creation**: Creates ClickHouse tables with optimal schemas
- **Data Deduplication**: Built-in duplicate prevention
- **Progress Tracking**: Real-time monitoring of data ingestion
- **Comprehensive Logging**: Structured logs for debugging and monitoring
- **Error Recovery**: Automatic retries with exponential backoff
- **Memory Efficient**: Streaming data processing for large datasets

## Development

The project uses Poetry for dependency management and follows Python best practices:

- Type hints throughout the codebase
- Structured logging with correlation IDs
- Async processing for optimal performance
- Modular architecture for easy maintenance

## Monitoring

Logs are written to:
- File-based logs in `logs/` directory
- ClickHouse for centralized log analysis
- Console output for real-time monitoring

## Architecture

The pipeline follows a producer-consumer pattern:
1. **Extract**: Query execution on QRadar consoles
2. **Transform**: Data cleaning and type conversion
3. **Load**: Batch insertion into ClickHouse

Each step is designed to be fault-tolerant and restartable.