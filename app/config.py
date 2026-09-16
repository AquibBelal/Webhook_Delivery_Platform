from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/webhooks"
    REDIS_URL: str = "redis://localhost:6379/0"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    MAX_RETRIES: int = 5
    INITIAL_BACKOFF_SECONDS: int = 2
    REQUEST_TIMEOUT_SECONDS: float = 5.0

    class Config:
        env_file = ".env"

settings = Settings()