from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AnyHttpUrl, constr, conint


class Settings(BaseSettings):
    """
    This module provides centralized configuration management for the application
    using Pydantic's `BaseSettings`. It automatically loads environment variables
    from the `.env` file.

    Configuration Settings:
    - QRadar Console IPs and Tokens
    - Kafka settings for the message queue
    - ClickHouse settings for the database
    - Impli project and API keys

    Validation is applied to ensure proper data types and values.
    To override settings for testing or development, specify a different `.env` file.
    """
    console_1_ip: str
    console_1_token: constr(min_length=10)  # Token must be at least 10 characters
    console_2_ip: str
    console_2_token: constr(min_length=10)
    console_3_ip: str
    console_3_token: constr(min_length=10)
    console_aa_ip: str
    console_aa_token: constr(min_length=10)
    console_uae_ip: str
    console_uae_token: constr(min_length=10)
    console_us_ip: str
    console_us_token: constr(min_length=10)

    kafka_host: str
    kafka_port: conint(ge=1, le=65535)  # Ensure the port is in a valid range

    max_attempts: conint(ge=1)  # Must be at least 1
    imply_project_id: str
    imply_api_key: constr(min_length=10)
    default_timeout: conint(ge=1)  # Timeout must be positive
    max_search_ttc_in_seconds: conint(ge=1)
    batch_size_limit: conint(ge=1)
    imply_base_url: AnyHttpUrl  # Ensure valid URL
    clickhouse_base_url: AnyHttpUrl
    clickhouse_batch_size: conint(ge=1)
    clickhouse_compression_protocol: str
    clickhouse_password: str
    clickhouse_port: conint(ge=1, le=65535)
    clickhouse_user: str
    max_queries_per_event_processor: conint(ge=1)
    max_event_processors_engaged: conint(ge=1)

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
