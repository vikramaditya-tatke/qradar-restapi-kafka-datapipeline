# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance ETL pipeline that extracts security event data from IBM QRadar SIEM consoles via REST API, transforms the data, and loads it into ClickHouse for analytics. The project has been recently refactored from a flat structure to a modular `src/` directory structure.

## Key Development Commands

### Environment Setup
```bash
# Install dependencies
poetry install

# Activate virtual environment (if needed manually)
source .venv/bin/activate
```

### Running the Pipeline
```bash
# Basic execution
python scripts/run.py --console <console_id> --max-threads <thread_count>

# Example: Run on US console with 5 threads
python scripts/run.py --console us --max-threads 5

# Available consoles: 1, 2, 3, aa, aus, uae, us
```

### Code Quality
```bash
# Format code
black .

# Lint code
pylint src/

# Type checking is configured for Pyright in pyproject.toml
```

## Architecture Overview

The codebase follows a 3-tier parallelization strategy:
1. **Process-level**: One process per event processor (multiprocessing)
2. **Thread-level**: Multiple threads per customer within each process
3. **Query-level**: Parallel execution of multiple queries per customer

### Core Components

**Pipeline Entry Point**: `scripts/run.py:176`
- Console mapping and argument parsing
- Orchestrates multiprocessing across event processors
- Handles command-line interface

**Data Flow Architecture**:
```
QRadar API → Extract → Transform → ClickHouse
     ↓         ↓         ↓         ↓
   qradar.py → executor.py → transformer.py → clickhouse.py
```

**Key Modules**:
- `src/clients/qradar.py` - QRadar REST API client with streaming JSON parsing
- `src/pipeline/executor.py` - Search execution with retry logic and status polling
- `src/pipeline/transformer.py` - ETL operations and data transformation
- `src/clients/clickhouse.py` - Async ClickHouse operations
- `src/utils/config.py` - Pydantic-based configuration management
- `src/models/attributes.py` - External configuration loading (queries, mappings)

## Configuration Management

### Environment Variables (`.env`)
The application uses Pydantic Settings for type-safe configuration:
- Console IPs and tokens: `console_{id}_ip`, `console_{id}_token`
- ClickHouse settings: `clickhouse_base_url`, `clickhouse_user`, etc.
- Performance tuning: `clickhouse_batch_size`, `max_attempts`, timeouts

### External Configuration Files
- `config/ep_clients.json` - Event processor to customer mappings
- `config/duration.json` - Time ranges for data extraction
- `config/queries.json` - AQL query definitions (loaded via `src/models/attributes.py`)

## Critical Development Patterns

### Memory Management
**ALWAYS** use streaming processing for large data sets:
- Use `ijson` for streaming JSON parsing from QRadar API responses
- Use Polars lazy frames (`.lazy()`) for data transformations
- **NEVER** call `response.json()` or `response.text()` on large API responses
- Process data in configurable batches (from `settings.clickhouse_batch_size`)

### Error Handling & Resilience
- All API calls use `tenacity` retry decorators with exponential backoff
- Status code specific retry logic in `src/pipeline/executor.py:27-49`
- Structured logging with context via `src.utils.logger`
- Graceful degradation for missing fields or failed queries

### Parallel Processing Implementation
The pipeline uses a specific parallelization pattern in `scripts/run.py`:
1. **Multiprocessing Pool** (`scripts/run.py:195`): One process per event processor
2. **ThreadPoolExecutor** (`scripts/run.py:114`): Multiple threads per customer
3. **Concurrent Query Execution**: Multiple queries executed per customer in parallel

## Database Integration

### ClickHouse Operations
- Schema-first table creation with optimal data types
- Async batch insertion for performance (`src/clients/clickhouse.py`)
- Data type mapping from QRadar fields to ClickHouse columns
- Automatic table creation based on query results

### Data Transformation
- ETL pipeline in `src/pipeline/transformer.py`
- Type conversion and data cleaning
- Field mapping and normalization
- Duplicate prevention and data quality validation

## Logging & Monitoring

### Structured Logging
Uses `loguru` with custom structured logging:
- Context fields: `ApplicationLog` (pipeline metadata), `QRadarLog` (API responses)
- Log levels: INFO (progress), WARNING (no data), ERROR (failures)
- Output destinations: Console, files (`logs/`), ClickHouse

### Performance Monitoring
- Real-time progress tracking via log output
- Record counts and timing information logged
- Memory-efficient streaming prevents OOM issues
- Configurable batch sizes for performance tuning

## Common Development Tasks

### Adding New QRadar Consoles
1. Add console IP and token to `.env` file
2. Update console mapping in `scripts/run.py:222-230`
3. Add validation in `src/utils/config.py:20-37`

### Modifying Query Logic
1. Update AQL queries in `config/queries.json`
2. Modify query builder logic in `src/pipeline/query_builder.py`
3. Test with small time ranges first

### Extending Data Transformations
1. Update transformation logic in `src/pipeline/transformer.py`
2. Ensure proper field mapping to ClickHouse types in `src/models/clickhouse_types.py`
3. Test with `clickhouse_batch_size = 10` for validation

## Important Notes

### Git Status Context
The repository is currently on `kafka_integration` branch with significant structural changes:
- Many old files have been deleted (marked with `D`)
- New `src/` directory structure implemented
- New `config/`, `data/`, and `scripts/` directories added

### Branch Strategy
- Current branch: `kafka_integration`
- Main branch: `master`
- Always create PRs against `master` branch

### Type Safety
- Python 3.11+ required
- Type hints used throughout codebase
- Pydantic models for configuration validation
- Pyright configuration in `pyproject.toml:32-34`

### Performance Considerations
- Default thread count: 5 per event processor (configurable via `--max-threads`)
- Batch sizes configured via environment variables
- Memory usage optimized through streaming and lazy evaluation
- ClickHouse compression enabled for network efficiency