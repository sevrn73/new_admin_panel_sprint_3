from typing import List, Tuple
from elasticsearch import Elasticsearch, helpers
from p_schemas import ESFilmworkData
from backoff import backoff


class ESLoad:
    def __init__(self, es_host: str, es_user: str, es_password: str):
        self.es = Elasticsearch(es_host, basic_auth=(es_user, es_password), verify_certs=False)

    @staticmethod
    @backoff()
    def send_data(es: Elasticsearch, es_data: List[ESFilmworkData]) -> Tuple[int, list]:
        query = [{"_index": "movies", "_id": data.id, "_source": data.dict()} for data in es_data]
        helpers.bulk(es, query)
