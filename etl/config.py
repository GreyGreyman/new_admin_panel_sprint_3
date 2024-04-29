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


class ScheduleSettings(BaseModel):
    """Настройки для запуска процесса etl по расписанию"""
    enabled: bool = False # по умолчанию запускать единоразово без использования планировщика
    cron: str # cron выражение
    timezone: str = "Europe/Moscow"


class Settings(BaseSettings):
    """Главный класс настроек всего приложения"""

    postgres: PostgresSettings
    es: ElasticSettings
    schedule: ScheduleSettings
    batch_size: int
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )


# Согласен что так удобнее, просто как-то комфортнее вызывать подобный код явно
# в главном потоке управления и явно передавать настройки по-назначению
settings = Settings()
