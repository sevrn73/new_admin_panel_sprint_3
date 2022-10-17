import os
import logging
import time
from contextlib import closing
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from load import ESLoad
from transform import parse_from_postgres_to_es
from extract import PSExtract
from state import JsonFileStorage, State

load_dotenv()
logging.basicConfig(
    filename='log.txt',
)
logger = logging.getLogger('loader')


def main(dsl: dict, es_connect: dict):
    """
    Функция запуска внутренних компонентов ETL
    """
    while True:
        try:
            with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn, pg_conn.cursor() as curs:
                storage = JsonFileStorage('etl/state.json')
                state = State(storage)
                last_modified = state.get_state('last_modified')
                offset = state.get_state('offset')

                postgres_loader = PSExtract(pg_conn, curs, offset)
                es_saver = ESLoad(**es_connect)
                table_names = ['film_work', 'person', 'genre']
                for table_name in table_names:
                    while True:
                        filmwork_ids = postgres_loader.get_filmwork_ids(table_name, last_modified)
                        if not filmwork_ids:
                            state.set_state('offset', 0)
                            break
                        filmwork_data = [
                            parse_from_postgres_to_es(data)
                            for data in postgres_loader.get_filmworks_data(filmwork_ids)
                        ]
                        postgres_loader.offset += len(filmwork_ids)
                        state.set_state('offset', postgres_loader.offset)

                        es_saver.send_data_to_es(es_saver.es, filmwork_data)

                    state.set_state('offset', 0)

                state.set_state('last_modified', datetime.now().strftime('%Y-%m-%d'))

        except psycopg2.OperationalError:
            logger.error('Error connecting to Postgres database')

        time.sleep(1)


if __name__ == '__main__':

    dsl = {
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': 'db',
        'port': 5432,
    }

    es_connect = {
        'es_host': os.environ.get('ES_HOST'),
        'es_user': os.environ.get('ES_USER'),
        'es_password': os.environ.get('ES_PASSWORD'),
    }
    main(dsl, es_connect)
