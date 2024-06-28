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
    console_aus_ip: str
    console_aus_token: str
    console_uae_ip: str
    console_uae_token: str
    console_us_ip: str
    console_us_token: str
    config = SettingsConfigDict(env_file=".env")


settings = Settings()
