import json
import logging
import typing as t
from pathlib import Path

import backoff
import elastic_transport
from config import ElasticSettings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from state import State

logger = logging.getLogger("etl")


class LoaderError(Exception): ...


ES_INDEX_PATH = Path(__file__).resolve().parent / "es_index.json"


class ESLoader:
    """Класс для загрузки данных в ES"""

    def __init__(
        self, es_config: ElasticSettings, index_name: str, state: State
    ) -> None:
        self.index_name = index_name
        self.state = state
        self.config = es_config
        self.client = Elasticsearch(f"http://{self.config.host}:{self.config.port}")

        # большое спасибо за ресурсы, обязательно изучу!
        self.ensure_index()

    @backoff.on_exception(
        backoff.expo,
        (elastic_transport.ConnectionError, elastic_transport.ConnectionTimeout),
        max_tries=10,
        max_time=10,
    )
    def ensure_index(self):
        if not self.client.indices.exists(index=self.index_name):
            with open("es_index.json", encoding="utf-8") as index_file:
                index_config = json.load(index_file)

            self.client.indices.create(index=self.index_name, body=index_config)

    @backoff.on_exception(
        backoff.expo,
        (elastic_transport.ConnectionError, elastic_transport.ConnectionTimeout),
        max_tries=10,
        max_time=10,
    )
    def _upload(self, load_actions):
        """Обёртка для backoff"""
        return bulk(self.client, load_actions)

    def load_batch(
        self,
        batch: list[dict[str, t.Any]],
    ):
        """
        Загружает пачками подготовленные для записи в elastisearch словари
        Ожидает наличие ключа "id"
        """
        load_actions = (
            {
                "_index": self.index_name,
                "_id": item["id"],
                "_source": item,
            }
            for item in batch
        )
        try:
            response = self._upload(load_actions)
        except Exception as e:
            # даже не думал об этом с точки зрения дискового пространства, спасибо!
            logging.exception(
                f"error while loading data into elasricsearch, problem batch: {batch}"
            )
            raise LoaderError from e
        else:
            logger.info(f"successufuly loaded {len(batch)} entries into elasticsearch")
