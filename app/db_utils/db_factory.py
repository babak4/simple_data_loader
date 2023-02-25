import pathlib
from datetime import datetime

import duckdb as db
from utils.generic import get_rendered_sql_file_contents

CURRENT_DIR = pathlib.Path().resolve()
DATA_PATH = f'{CURRENT_DIR}/data'
DB_PATH = f'{CURRENT_DIR}/db'
SQL_PATH = f'{CURRENT_DIR}/sql'


class DB:
    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self.set_connection()

    def initialize_db_schemas(self) -> None:
        raise NotImplementedError('Subclass must implement this method')

    def set_connection(self) -> None:
        raise NotImplementedError('Subclass must implement this method')

    def _run_sql_statement(self, sql_statement: str) -> None:
        raise NotImplementedError('Subclass must implement this method')

    def _run_sql_statement_return_rowcount(self, sql_statement: str) -> int:
        raise NotImplementedError('Subclass must implement this method')

    def stage_aggregate_view_events(self, data_dir: str) -> int:
        raise NotImplementedError('Subclass must implement this method')

    def clear_aggregate_view_events(self, date_hour: datetime) -> int:
        raise NotImplementedError('Subclass must implement this method')

    def load_aggregate_view_events_from_staging(self, date_hour: datetime) -> int:
        raise NotImplementedError('Subclass must implement this method')


class DuckDB(DB):
    def initialize_db_schemas(self) -> None:
        if not self._conn or not isinstance(self._conn, db.DuckDBPyConnection):
            raise ValueError(
                'The `conn` parameter is supplied with an inappripriate value!'
            )

        self._run_sql_statement(
            sql_statement=get_rendered_sql_file_contents(
                file_path=SQL_PATH,
                sql_name='create_table_viewed_events_hourly',
            )
        )

    def set_connection(self) -> None:
        self._conn = db.connect(str(pathlib.PurePath(DB_PATH, self._connection_string)))
        self.initialize_db_schemas()

    def _run_sql_statement(self, sql_statement: str) -> None:
        self._conn.execute(sql_statement)

    def _run_sql_statement_return_rowcount(self, sql_statement: str) -> int:
        return self._conn.execute(sql_statement).fetchall()[0][0]

    def stage_aggregate_view_events(self, data_dir: str) -> int:
        sql_statement = get_rendered_sql_file_contents(
            file_path=SQL_PATH,
            sql_name='create_temp_table_tmp_hourly_aggregates',
            parameters={'data_dir': data_dir},
        )
        return self._run_sql_statement_return_rowcount(sql_statement)

    def clear_aggregate_view_events(self, date_hour: datetime) -> int:
        sql_statement = get_rendered_sql_file_contents(
            file_path=SQL_PATH,
            sql_name='delete_from_viewed_events_hourly',
            parameters={'date_hour': date_hour.strftime('%Y-%m-%d %H:00:00')},
        )
        return self._run_sql_statement_return_rowcount(sql_statement)

    def load_aggregate_view_events_from_staging(self, date_hour: datetime) -> int:
        sql_statement = get_rendered_sql_file_contents(
            file_path=SQL_PATH,
            sql_name='ias_viewed_events_hourly_tmp_hourly',
            parameters={'date_hour': date_hour.strftime('%Y-%m-%d %H:00:00')},
        )
        return self._run_sql_statement_return_rowcount(sql_statement)


class DBFactory:
    def get_db(self, db_type, connection_string: str) -> DB:
        if db_type == 'DuckDB':
            return DuckDB(connection_string)
        else:
            raise ValueError('Invalid DB type')
