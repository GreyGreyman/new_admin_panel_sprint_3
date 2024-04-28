from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent


class PostgresSettings(BaseModel):
    """Настройки подключения к PostgreSQL"""

    db: str
    user: str
    password: str
    host: str
    port: str


class ElasticSettings(BaseModel):
    """Настрокий подключения к elasctisearch"""

    host: str
    port: str


class Settings(BaseSettings):
    """Главный класс настроек всего приложения"""

    postgres: PostgresSettings
    es: ElasticSettings
    batch_size: int
    update_cooldown: int

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )


if __name__ == "__main__":
    settings = Settings()
    print(settings)
