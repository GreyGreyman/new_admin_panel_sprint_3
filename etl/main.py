import logging
import time
from dataclasses import dataclass

from config import PROJECT_ROOT, Settings
from extractor import (
    FILMWORK_SQL,
    GENRES_SQL,
    PERSONS_SQL,
    PgExtractor,
)
from loader import ESLoader, LoaderError
from logger import setup_logger
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
                f"ошибка при обработке пачки {filmwork_batch}, прерываем загрузку"
            )
            raise e
        else:
            state.set_state(entity.state_key, filmwork_batch[-1].modified.isoformat())


def main():
    setup_logger()
    try:
        settings = Settings()
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

        while True:
            logger.info("запуск etl процесса")
            with pg_extractor:
                for entity in entities:
                    run_etl(entity, pg_extractor, es_loader, state)
                    logger.info(
                        f"обработаны обновления кинопроизведений из сущности {entity.name}"
                    )
            time.sleep(settings.update_cooldown)

    except Exception:
        logger.exception("необработанное исключение")


if __name__ == "__main__":
    main()
