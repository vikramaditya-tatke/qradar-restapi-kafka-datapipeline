---
name: qradar-restapi-kafka-datapipeline
description: ETL pipeline for extracting security event data from QRadar REST API, transforming it, and loading into ClickHouse for analytics and downstream processing.
---

# QRadar REST API Kafka Data Pipeline

## Project Overview

This project implements a high-performance ETL (Extract, Transform, Load) pipeline that:
- **Extracts** security event data from IBM QRadar SIEM via REST API
- **Transforms** raw event data with schema normalization and enrichment
- **Loads** processed data into ClickHouse database for fast analytics
- Supports parallel processing across multiple event processors and customers
- Uses streaming JSON parsing for memory-efficient data handling

## Technology Stack

**Core Technologies:**
- **Python 3.11+** - Primary programming language
- **ClickHouse** - High-performance columnar database for analytics
- **QRadar REST API** - IBM SIEM platform for security event collection
- **Kafka** - Message streaming platform (via confluent-kafka)

**Key Libraries:**
- `requests` - HTTP client for QRadar API communication
- `clickhouse-connect` / `clickhouse-driver` - ClickHouse database clients
- `polars` - Fast DataFrame library for data transformations (always use lazyframes)
- `ijson` - Streaming JSON parser for handling large API responses
- `loguru` - Structured logging with context
- `pydantic-settings` - Configuration management with validation
- `tenacity` - Retry logic for resilient API calls

## Architecture

### Pipeline Flow
```
QRadar API → Extract Batches → Transform → Load ClickHouse
     ↓
Multi-threaded Query Execution
     ↓
Multi-process Event Processor Handling
```

### Key Components

1. **QRadar Connector** (`qradar/qradarconnector.py`)
   - Manages authentication and API sessions
   - Executes AQL queries against QRadar
   - Streams large result sets using cursor-based pagination

2. **ETL Pipeline** (`etl.py`)
   - Batch processing with configurable batch sizes
   - Schema-first table creation in ClickHouse
   - Async data loading for improved throughput

3. **Pipeline Orchestration** (`run.py`)
   - Multi-process execution per event processor
   - Multi-threaded query execution per customer
   - Centralized error handling and logging

4. **ClickHouse Integration** (`clickhouse/`)
   - Dynamic table creation with proper data types
   - Batch insertion for optimal performance
   - Field mapping and transformation helpers

## Development Environment

### Python Environment
- **Virtual Environment**: `.venv/bin/python`
- **Package Manager**: Poetry
- **Python Version**: 3.11+

### Dependency Management
```bash
# Install dependencies
poetry install

# Add new dependency
poetry add <package-name>

# Update dependencies
poetry update
```

### Code Quality Tools
- **Formatter**: Black (configured in pyproject.toml)
- **Linter**: Pylint
- **Type Checking**: Pyright (venv configured in pyproject.toml)

## Configuration

### Environment Variables (`.env`)
Required configuration stored in `.env` file:
- QRadar credentials and endpoints
- ClickHouse connection details
- Batch sizes and performance tuning parameters
- Logging configuration

### Settings Management
Configuration loaded via `settings.py` using pydantic-settings:
- Type-safe configuration validation
- Environment variable injection
- Default value management

### Query Attributes (`attributes.py`)
External configuration file for:
- Customer-to-event-processor mapping
- AQL query definitions
- Time ranges for data extraction

## Data Processing Guidelines

### Memory Management
- **ALWAYS use Polars lazyframes** when processing API responses
- **NEVER load entire responses** into memory using `response.json()` or `response.text()`
- Use `ijson` for streaming JSON parsing from QRadar API
- Process data in configurable batch sizes (default: from settings)

### ClickHouse Best Practices
- Let the ETL pipeline handle table creation (schema-first approach)
- Batch sizes configured via `settings.clickhouse_batch_size`
- Async operations for improved throughput
- Proper data type mapping from QRadar fields to ClickHouse columns

### Error Handling
- Comprehensive exception handling at each pipeline stage
- Structured logging with context (ApplicationLog, QRadarLog)
- Retry logic for transient failures (via tenacity)
- Graceful degradation for missing fields

## Execution Patterns

### Running the Pipeline

**Basic Execution:**
```bash
.venv/bin/python run.py --console <console_name> --max-threads <thread_count>
```

**Development Testing:**
```bash
# Single customer test
.venv/bin/python run.py --console dev --max-threads 2

# Full production run
.venv/bin/python run.py --console prod --max-threads 8
```

### Parallelization Strategy

1. **Process-level**: One process per event processor (multi-processing)
2. **Thread-level**: Multiple threads per customer (multi-threading)
3. **Query-level**: Parallel execution of queries per customer

This 3-tier parallelization enables efficient resource utilization for high-volume data extraction.

## Logging

### Structured Logging
- **Library**: loguru with custom pipeline_logger
- **Format**: JSON-structured logs with contextual metadata
- **Levels**: INFO (progress), WARNING (no data), ERROR (failures)
- **Context Fields**:
  - `ApplicationLog`: Pipeline metadata (customer, query, timing)
  - `QRadarLog`: API response headers (cursor_id, record_count)

### Log Output
- Console output for development
- File-based logging in `logs/` directory
- Integration with python-logstash-async for centralized logging

## ClickHouse Utilities

### Using ClickHouse CLI in Warp
When in agent mode, use the `chcl` alias for quick data exploration:

```bash
# Query CSV files directly
chcl --query "SELECT * FROM file('data.csv') GROUP BY field ORDER BY field"

# Explore table structure
chcl --query "DESCRIBE TABLE database.table_name"

# Sample data inspection
chcl --query "SELECT * FROM database.table_name LIMIT 100"
```

Alternatively, use the ClickHouse MCP server tool calls for programmatic queries.

## Project Structure

```
qradar-restapi-kafka-datapipeline/
├── .venv/                    # Python virtual environment
├── qradar/                   # QRadar API integration
│   ├── qradarconnector.py   # API client and authentication
│   └── search_executor.py   # Query execution logic
├── clickhouse/               # ClickHouse integration
│   ├── clickhouse.py        # Database operations
│   └── helpers.py           # Data transformation utilities
├── etl.py                    # ETL pipeline orchestration
├── run.py                    # Entry point and parallelization
├── pipeline_logger.py        # Structured logging configuration
├── settings.py               # Configuration management
├── attributes.py             # Query and mapping definitions
├── docker-compose.yml        # Local ClickHouse setup (if needed)
├── pyproject.toml            # Poetry dependencies and config
└── .env                      # Environment variables (not in git)
```

## Common Tasks

### Adding New Queries
1. Edit `attributes.py` to define new AQL queries
2. Map customer to event processor if needed
3. Run pipeline with test data first
4. Verify table creation and data in ClickHouse

### Modifying Data Transformations
1. Update transformation logic in `clickhouse/helpers.py`
2. Ensure proper field mapping to ClickHouse types
3. Test with small batch first (`settings.clickhouse_batch_size = 10`)
4. Validate data quality in target tables

### Debugging Pipeline Issues
1. Check structured logs in `logs/` directory
2. Use ClickHouse MCP server to verify data state
3. Test individual components in isolation (QRadar connector, ETL, ClickHouse)
4. Enable debug logging for detailed trace

### Performance Tuning
- Adjust `settings.clickhouse_batch_size` for optimal throughput
- Tune thread count based on API rate limits and system resources
- Monitor ClickHouse memory usage and query performance
- Consider partitioning large tables by date

## Best Practices

### Code Style
- Follow Black formatting standards (automated via pyproject.toml)
- Use type hints for function signatures
- Document complex transformations and business logic
- Keep functions focused and single-purpose

### Data Quality
- Validate required fields before ClickHouse insertion
- Handle missing or null values gracefully
- Log data quality issues for investigation
- Use pydantic models for schema validation where appropriate

### Security
- Never commit `.env` file or credentials
- Use environment variables for all secrets
- Rotate QRadar tokens regularly
- Restrict ClickHouse permissions appropriately

### Testing
- Test with small time ranges first (e.g., 1 hour of data)
- Verify schema compatibility before large runs
- Monitor initial batches for errors
- Use `.venv/bin/python` for all script executions

## Troubleshooting

### Memory Issues
- Reduce batch sizes in settings
- Ensure streaming JSON parsing is working (ijson)
- Check for Polars eager evaluation (use lazyframes)
- Monitor system resources during execution

### QRadar API Errors
- Verify token validity and permissions
- Check AQL query syntax
- Review rate limiting and timeouts
- Examine QRadar logs for server-side issues

### ClickHouse Issues
- Verify connection parameters in `.env`
- Check table schema compatibility
- Review ClickHouse server logs
- Ensure sufficient disk space for data

### Pipeline Failures
- Review structured logs for exception traces
- Check customer-event-processor mappings
- Verify query definitions in attributes
- Test individual pipeline stages in isolation

## Future Enhancements

Potential improvements for this pipeline:
- Add data quality metrics and monitoring
- Implement incremental updates (upsert logic)
- Add real-time streaming via Kafka producers
- Create data validation framework
- Build observability dashboard (Streamlit available)
- Add unit and integration tests

## Support & Documentation

- Project documentation: This WARP.md file
- QRadar API docs: [IBM QRadar REST API Documentation]
- ClickHouse docs: [ClickHouse Official Documentation]
- Internal queries: See `attributes.py` for query definitions
- Logging: Check `logs/` directory for execution history
