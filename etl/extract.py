from typing import Optional, Tuple
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from backoff import backoff


class PSExtract:
    LIMIT_ROWS = 100

    def __init__(self, pg_conn: _connection, curs: DictCursor, offset: int) -> None:
        self.pg_conn = pg_conn
        self.curs = curs
        self.offset = offset

    @staticmethod
    @backoff()
    def extract_data(query: str, curs: DictCursor):
        """
        Метод загрузки данных из Postgres

        Parameters
        ----------
        :param query: запрос к БД
        :param curs: курсор Postgres
        ----------
        """
        curs.execute(query)
        data = curs.fetchall()
        return data

    @staticmethod
    @backoff()
    def extract_full_data(query: str, curs: DictCursor) -> list:
        curs.execute(query)
        data_row = curs.fetchall()
        columns = [data[0] for data in curs.description]
        data = [dict(zip(columns, row)) for row in data_row]
        return data

    def get_ids_from_table(self, table_name: str, last_modified: str):
        query = (
            f'SELECT id from content.{table_name} '
            f"WHERE modified > '{last_modified}' "
            'ORDER BY modified '
            f'LIMIT {self.LIMIT_ROWS} OFFSET {self.offset}'
        )
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids_by_persons(self, persons_ids: Tuple[str]) -> Tuple[str]:
        query = (
            'SELECT fw.id FROM content.film_work fw '
            'LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id '
            f'WHERE pfw.person_id IN {persons_ids} '
            'ORDER BY fw.modified'
        )
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids_by_genres(self, genres_ids: Tuple[str]) -> Tuple[str]:
        query = (
            'SELECT fw.id FROM content.film_work fw '
            'LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id '
            f'WHERE gfw.genre_id in {genres_ids} '
            'ORDER BY fw.modified '
        )
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids(self, table_name: str, last_modified: str) -> Optional[Tuple[str]]:
        objects_ids = self.get_ids_from_table(table_name, last_modified)
        if table_name == 'film_work':
            return objects_ids
        elif table_name == 'person':
            return self.get_filmwork_ids_by_persons(objects_ids) if objects_ids else None
        elif table_name == 'genre':
            return self.get_filmwork_ids_by_genres(objects_ids) if objects_ids else None

    def get_filmworks_data(self, filmwork_ids: Tuple[str]) -> list:
        if len(filmwork_ids) == 1:
            where_condition = f"WHERE fw.id = '{filmwork_ids[0]}' "
        else:
            where_condition = f'WHERE fw.id IN {filmwork_ids} '
        query = (
            'SELECT fw.id as fw_id, fw.title, fw.description, '
            'fw.rating, fw.type, fw.created, fw.modified, '
            "COALESCE ( \
                json_agg( \
                    DISTINCT jsonb_build_object( \
                        'person_id', p.id, \
                        'role', pfw.role, \
                        'full_name', p.full_name \
                    ) \
                ) FILTER (WHERE p.id is not null), \
                '[]' \
            ) as persons, "
            'array_agg(DISTINCT g.name) as genres '
            'FROM content.film_work fw '
            'LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id '
            'LEFT JOIN content.person p ON p.id = pfw.person_id '
            'LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id '
            'LEFT JOIN content.genre g ON g.id = gfw.genre_id '
            f'{where_condition}'
            'GROUP BY fw.id;'
        )
        data = self.extract_data(query, self.curs)
        return data
