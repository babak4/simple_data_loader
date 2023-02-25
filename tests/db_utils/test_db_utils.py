import os
from unittest.mock import patch

import duckdb
import pytest

from app.db_utils import db_factory

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def test_DBFactory_get_db_fail_invalid_db_type():
    with pytest.raises(ValueError):
        _ = db_factory.DBFactory().get_db('PotGreSQL', 'test_dwh.db')


@patch('app.db_utils.db_factory.DB_PATH', CURRENT_DIR)
def test_DBFactory_get_db_sucsess_DuckDB():
    if os.path.exists(os.path.join(CURRENT_DIR, 'test_dwh.db')):
        os.remove(os.path.join(CURRENT_DIR, 'test_dwh.db'))

    db = db_factory.DBFactory().get_db('DuckDB', 'test_dwh.db')
    assert db._connection_string == 'test_dwh.db'
    assert db._conn
    assert isinstance(db._conn, duckdb.DuckDBPyConnection)
    assert os.path.exists(os.path.join(CURRENT_DIR, 'test_dwh.db'))
    assert (
        db._run_sql_statement_return_rowcount(
            'SELECT COUNT(*) FROM viewed_events_hourly'
        )
        == 0
    )
    with pytest.raises(duckdb.CatalogException):
        assert (
            db._run_sql_statement_return_rowcount(
                'SELECT COUNT(*) FROM tmp_hourly_aggregates'
            )
            == 0
        )
