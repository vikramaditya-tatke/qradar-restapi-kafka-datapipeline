from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    console_1_ip: str
    console_1_token: str
    console_2_ip: str
    console_2_token: str
    console_3_ip: str
    console_3_token: str
    console_aa_ip: str
    console_aa_token: str
    console_uae_ip: str
    console_uae_token: str
    console_us_ip: str
    console_us_token: str
    kafka_host: str
    kafka_port: int
    max_attempts: int
    imply_project_id: str
    imply_api_key: str
    default_timeout: int
    max_search_ttc_in_seconds: int
    batch_size_limit: int
    imply_base_url: str
    clickhouse_base_url: str
    clickhouse_batch_size: int
    clickhouse_compression_protocol: str
    clickhouse_password: str
    clickhouse_port: int
    clickhouse_user: str
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
