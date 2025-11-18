from typing import Annotated
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    This module provides centralized configuration management for the application
    using Pydantic's `BaseSettings`. It automatically loads environment variables
    from the `.env` file.

    Configuration Settings:
    - QRadar Console IPs and Tokens
    - Kafka settings for the message queue
    - ClickHouse settings for the database

    Validation is applied to ensure proper data types and values.
    To override settings for testing or development, specify a different `.env` file.
    """

    console_1_ip: str
    console_1_token: Annotated[str, Field(min_length=10)]
    console_2_ip: str
    console_2_token: Annotated[str, Field(min_length=10)]
    console_3_ip: str
    console_3_token: Annotated[str, Field(min_length=10)]
    console_aa_ip: str
    console_aa_token: Annotated[str, Field(min_length=10)]
    console_aus_ip: str
    console_aus_token: Annotated[str, Field(min_length=10)]
    console_uae_ip: str
    console_uae_token: Annotated[str, Field(min_length=10)]
    console_us_ip: str
    console_us_token: Annotated[str, Field(min_length=10)]
    console_ind_ip: str
    console_ind_token: Annotated[str, Field(min_length=10)]
    console_sa_ip: str
    console_sa_token: Annotated[str, Field(min_length=10)]

    max_attempts: Annotated[int, Field(ge=1)]
    default_timeout: Annotated[int, Field(ge=1)]
    max_search_ttc_in_seconds: Annotated[int, Field(ge=1)]

    clickhouse_base_url: str
    clickhouse_batch_size: Annotated[int, Field(ge=2)]
    clickhouse_compression_protocol: str
    clickhouse_password: str
    clickhouse_port: Annotated[int, Field(ge=1, le=65535)]
    clickhouse_database: str
    clickhouse_user: str
    max_queries_per_event_processor: Annotated[int, Field(ge=1)]
    max_event_processors_engaged: Annotated[int, Field(ge=1)]

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent.parent / ".env")
    )


settings = Settings.model_validate({})
