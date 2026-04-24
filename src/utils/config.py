from typing import Annotated, Any
from pathlib import Path
from pydantic import Field, BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

# Load environment variables from .env file into os.environ
loaded = load_dotenv(Path(__file__).parent.parent.parent / ".env", override=True)
print(f"DEBUG: load_dotenv result: {loaded}")


class ConsoleConfig(BaseModel):
    ip: str
    token: Annotated[str, Field(min_length=10)]


class Settings(BaseSettings):
    """
    This module provides centralized configuration management for the application
    using Pydantic's `BaseSettings`. It automatically loads environment variables
    from the `.env` file.

    Configuration Settings:
    - QRadar Console IPs and Tokens (Dynamic)
    - Kafka settings for the message queue
    - ClickHouse settings for the database

    Validation is applied to ensure proper data types and values.
    To override settings for testing or development, specify a different `.env` file.
    """

    # Dynamic dictionary of consoles: {"1": ConsoleConfig(...), "us": ConsoleConfig(...)}
    consoles: dict[str, ConsoleConfig] = Field(default_factory=dict)

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

    # New setting for SSL verification
    verify_ssl: bool = False
    clickhouse_secure: bool = False

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent.parent / ".env"),
        extra="ignore",  # Ignore extra fields that are not defined in the model
    )

    @model_validator(mode="before")
    @classmethod
    def parse_consoles(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        consoles = {}

        # Source to look for console variables: data (if passed explicitly) and os.environ
        sources = [data]
        import os

        sources.append(dict(os.environ))

        for source in sources:
            if not isinstance(source, dict):
                continue

            for key, value in source.items():
                key_lower = key.lower()
                if key_lower.startswith("console_") and (
                    "_ip" in key_lower or "_token" in key_lower
                ):
                    parts = key_lower.split("_")
                    # Expected format: console_{name}_ip or console_{name}_token
                    # But name could contain underscores? Assuming simple names for now based on existing config.
                    # Existing names: 1, 2, aa, aus, uae, us, ind, sa, afg (no underscores in names)

                    if len(parts) == 3:
                        name = parts[1]
                        field = parts[2]  # ip or token

                        if name not in consoles:
                            consoles[name] = {}

                        consoles[name][field] = value

        # Convert dicts to ConsoleConfig objects
        final_consoles = {}
        for name, config in consoles.items():
            if "ip" in config and "token" in config:
                final_consoles[name] = ConsoleConfig(
                    ip=config["ip"], token=config["token"]
                )

        data["consoles"] = final_consoles
        return data


settings = Settings.model_validate({})
