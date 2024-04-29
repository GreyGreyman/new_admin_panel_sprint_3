import logging
import time
from dataclasses import dataclass

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from config import PROJECT_ROOT, settings
from extractor import PgExtractor
from loader import ESLoader, LoaderError
from logger import setup_logger
from sql_queries import FILMWORK_SQL, GENRES_SQL, PERSONS_SQL
from state import JsonFileStorage, State
from transformer import TransformError, transform_filmworks

logger = logging.getLogger("etl")


@dataclass
class EntityETLConfig:
    name: str
    sql: str
    state_key: str


def run_etl(
    entity: EntityETLConfig, extractor: PgExtractor, loader: ESLoader, state: State
):
    extract_batch_iterator = extractor.extract_filmworks(
        entity.sql,
        entity.state_key,
    )
    for filmwork_batch in extract_batch_iterator:
        try:
            transformed_filmworks = transform_filmworks(filmwork_batch)
            loader.load_batch(transformed_filmworks)
        except (LoaderError, TransformError) as e:
            logger.error(
                f"error while processing batch {filmwork_batch}, aborting etl process"
            )
            raise e
        else:
            state.set_state(entity.state_key, filmwork_batch[-1].modified.isoformat())


def etl():
    try:
        logger.info("starting etl process")
        state = State(JsonFileStorage(PROJECT_ROOT / "state/state.json"))
        entities = [
            EntityETLConfig(
                name="filmwork", sql=FILMWORK_SQL, state_key="filmworks_modified"
            ),
            EntityETLConfig(
                name="person", sql=PERSONS_SQL, state_key="persons_modified"
            ),
            EntityETLConfig(name="genre", sql=GENRES_SQL, state_key="genres_modified"),
        ]
        pg_extractor = PgExtractor(settings.postgres, settings.batch_size, state)
        es_loader = ESLoader(settings.es, index_name="movies", state=state)
        with pg_extractor:
            for entity in entities:
                run_etl(entity, pg_extractor, es_loader, state)
                logger.info(f"processed filmworks updates from entity {entity.name}")
    except Exception:
        logger.exception("unhadled exception")


# знаю про библиотеку apscheduler и использую в работе, просто по теории и комментариям наставников
# я решил что способ запуска нашего процесса etl за рамками учебного проекта и сделал while True sleep
# как минимальный способ тестирования и демонстрации обработки обновлений
# на самом деле я бы предпочел использовать для планирования запусков именно cron или systemd timers
# настроил одноразовый запуск по-умолчанию чтобы можно было использовать внешинй планировщик (cron)
# или запуск с использованием apscheduler при наличии переменной среды SCHEDULE__ENABLED, для настройки
# расписания использовал выражения крон как наиболее простой и быстрый способ гибкой настройки
def main():
    setup_logger()

    if not settings.schedule.enabled:
        etl()
    else:
        cron_expression = settings.schedule.cron
        schedule_timezone = settings.schedule.timezone
        trigger = CronTrigger.from_crontab(cron_expression, timezone=schedule_timezone)
        scheduler = BackgroundScheduler()
        scheduler.add_job(etl, trigger)
        logger.info(
            f"starting scheduler with cron: {cron_expression} timezone: {schedule_timezone}",
        )
        scheduler.start()

        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()


if __name__ == "__main__":
    main()
