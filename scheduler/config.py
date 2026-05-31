from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "OTEE Task Scheduler"
    ENV: str = "development"
    DATABASE_URL: str
    JOB_TIMEOUT_SECONDS: int = 30
    SWEEPER_INTERVAL_SECONDS: int = 5
    WORKER_POLL_INTERVAL_SECONDS: float = 2.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
