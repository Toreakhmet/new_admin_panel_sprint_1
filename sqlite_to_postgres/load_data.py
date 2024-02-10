import io
import logging
import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from data_classes import Movie, Person, Genre, GenreFilmWork, PersonFilmWork
from contextlib import closing
load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

BLOCK_SIZE = 100
TABLES_TO_CLASSES = {
    'film_work': Movie,
    'genre': Genre,
    'person': Person,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork,
}


class SQLiteLoader:
    def __init__(self, connection, table_name, data_class, verbose=False):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.verbose = verbose
        self.table_name = table_name
        self.data_class = data_class
        self.cursor.execute(f'SELECT * FROM {self.table_name}')

    def load_table(self, BLOCK_SIZE=100):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT * FROM {self.table_name}')
            counter = 0
            while True:
                block_rows = cursor.fetchmany(size=BLOCK_SIZE)
                if not block_rows:
                    break
                block = [self.data_class(*row) for row in block_rows]
                yield block
                counter += 1

            if self.verbose:
                log.info('Загружено: из %s %s блоков', self.table_name, counter)
        except Exception as e:
            log.error("Произошла ошибка при загрузке данных: %s", e)
            raise
        finally:
                cursor.close()


class PostgresSaver(SQLiteLoader):

    def save_all_data(self, data):
        counter = 0

        for block in data:
            block_values = '\n'.join([obj.get_values for obj in block])
            with io.StringIO(block_values) as f:
                self.cursor.copy_from(f, table=self.table_name, null='None', size=BLOCK_SIZE)
            counter += 1

        if self.verbose:
            log.info('В таблицу %s вставлено: %s блоков', self.table_name, counter)


def load_from_sqlite(sql_conn: sqlite3.Connection, psg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""

    for table_name, data_class in TABLES_TO_CLASSES.items():
        try:
            sqlite_loader = SQLiteLoader(sql_conn, table_name, data_class, verbose=True)
            data = sqlite_loader.load_table()
        except Exception:
            break
        try:
            postgres_saver = PostgresSaver(psg_conn, table_name, data_class, verbose=True)
            postgres_saver.save_all_data(data)
        except Exception:
            break


if __name__ == '__main__':
    dsl = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('DB_DEV_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'options': '-c search_path=content'
    }
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        # Использование contextlib.closing для PostgreSQL
        try:
            with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
                load_from_sqlite(sqlite_conn, pg_conn)
        except Exception as e:
            print(f"Произошла ошибка: {e}")