import logging
import typing as t
from textwrap import dedent

import backoff
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from config import PostgresSettings
from models import Filmwork
from state import State

logger = logging.getLogger("etl")


INITIAL_DATE = "-infinity"


class PgExtractor:
    """Класс для загрузки кинопроизведений в postgres"""

    def __init__(
        self, pg_config: PostgresSettings, batch_size: int, state: State
    ) -> None:
        self.batch_size = batch_size
        self.pg_config = pg_config
        self.state = state

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        max_tries=10,
        max_time=10,
    )
    def open(self) -> None:
        """Установка соединения с PostgreSQL"""
        self.connection: psycopg2.extensions.connection = psycopg2.connect(
            dbname=self.pg_config.db,
            user=self.pg_config.user,
            password=self.pg_config.password,
            host=self.pg_config.host,
            port=self.pg_config.port,
            cursor_factory=psycopg2.extras.DictCursor,
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> t.Self:
        """Метод для использования класса в качества контекстного менеджера"""
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Метод для использования класса в качества контекстного менеджера"""
        self.close()

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        max_tries=10,
        max_time=10,
    )
    def extract_filmworks(
        self,
        sql: str,
        state_key: str,
    ) -> t.Iterator[list[Filmwork]]:
        """Загружает пачками из postgres кинопроизведения"""
        state_modified = self.state.get_state(state_key)

        # для первой загрузки при пустом state
        if not state_modified:
            state_modified = INITIAL_DATE

        with self.connection.cursor() as cursor:
            cursor.execute(sql, (state_modified,))
            while batch := cursor.fetchmany(self.batch_size):
                logger.info(f"получено {len(batch)} новых записей из postgres")
                yield [Filmwork(**filmwork) for filmwork in batch]


FILMWORK_SQL = dedent("""
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   fw.modified,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'role', pfw.role,
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > %s
GROUP BY fw.id
ORDER BY fw.modified
""")

PERSONS_SQL = dedent("""
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   MAX(p.modified) as modified ,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'role', pfw.role,
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE p.modified > %s
GROUP BY fw.id
ORDER BY modified
""")

GENRES_SQL = dedent("""
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   MAX(g.modified) as modified ,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'role', pfw.role,
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE g.modified > %s
GROUP BY fw.id
ORDER BY modified
""")
