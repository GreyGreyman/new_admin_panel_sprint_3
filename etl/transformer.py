import logging
import typing as t

import pydantic
from models import Filmwork

logger = logging.getLogger("etl")


# хардкод ролей, потому что мы ожидаем увидеть список именно этих ролей
# в документе es, даже если они будут пустыми
ROLE_TYPES = ("director", "writer", "actor")


class TransformError(Exception): ...


def transform_filmworks(filmworks: list[Filmwork]) -> list[dict[str, t.Any]]:
    return [transform_filmwork_to_es(filmwork) for filmwork in filmworks]


def transform_filmwork_to_es(filmwork: Filmwork) -> dict[str, t.Any]:
    """Функция для получения из объекта Filmwork данных для загрузки в Elasticserch"""
    try:
        es_filmwork = filmwork.model_dump(exclude={"persons", "modified"})

        for role in ROLE_TYPES:
            es_filmwork[f"{role}s"] = [
                person.model_dump(include={"id", "name"})
                for person in filmwork.persons
                if person.role == role
            ]
            es_filmwork[f"{role}s_names"] = [
                person.name for person in filmwork.persons if person.role == role
            ]
    except pydantic.ValidationError as e:
        logger.error(f"ошибка при транформации кинопроизведения: {filmwork}")
        raise TransformError from e

    return es_filmwork
