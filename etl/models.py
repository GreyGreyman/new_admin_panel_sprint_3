from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class Person(BaseModel):
    id: UUID
    name: str
    role: str


class Filmwork(BaseModel):
    id: UUID
    imdb_rating: float | None = Field(validation_alias="rating", default=None)
    genres: list[str]
    title: str
    description: str | None = None
    persons: list[Person]
    modified: datetime
