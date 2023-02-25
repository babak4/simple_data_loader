import os
from datetime import date, datetime
from unittest.mock import Mock, patch

import task

from app.db_utils import db_factory

SAMPLE_DATE = date(2023, 1, 1)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_LOCATION = f'{CURRENT_DIR}/data'

date_hour = datetime.combine(SAMPLE_DATE, datetime.min.time()).replace(hour=0)


def test_num_of_event_views_per_hour_success_correct_calls():
    mock_db_object = Mock()
    task.num_of_event_views_per_hour(
        date_hour=date_hour, data_path=TEST_DATA_LOCATION, db=mock_db_object
    )
    mock_db_object.stage_aggregate_view_events.assert_called_once_with(
        f'{TEST_DATA_LOCATION}/2023/01/01/00'
    )


@patch('app.db_utils.db_factory.DB_PATH', CURRENT_DIR)
def test_num_of_event_views_per_hour_success_correct_data():
    global db
    if os.path.exists(os.path.join(CURRENT_DIR, 'test_dwh.db')):
        os.remove(os.path.join(CURRENT_DIR, 'test_dwh.db'))
    db = db_factory.DBFactory().get_db('DuckDB', 'test_dwh.db')
    task.num_of_event_views_per_hour(
        date_hour=date_hour, data_path=TEST_DATA_LOCATION, db=db
    )
    assert (
        db._run_sql_statement_return_rowcount(
            'SELECT COUNT(*) FROM tmp_hourly_aggregates'
        )
        == 7
    )


def test_num_of_event_views_per_hour_success_assert_idempotency():
    task.num_of_event_views_per_hour(
        date_hour=date_hour, data_path=TEST_DATA_LOCATION, db=db
    )
    task.num_of_event_views_per_hour(
        date_hour=date_hour, data_path=TEST_DATA_LOCATION, db=db
    )
    assert (
        db._run_sql_statement_return_rowcount(
            'SELECT COUNT(*) FROM tmp_hourly_aggregates'
        )
        == 7
    )


def test_write_to_database_success_correct_calls():
    mock_db_object = Mock()
    task.write_to_database(date_hour=date_hour, db=mock_db_object)
    mock_db_object.clear_aggregate_view_events.assert_called_once_with(date_hour)
    mock_db_object.load_aggregate_view_events_from_staging.assert_called_once_with(
        date_hour
    )


def test_write_to_database_success_correct_data():
    task.write_to_database(date_hour=date_hour, db=db)
    assert (
        db._run_sql_statement_return_rowcount(
            """SELECT COUNT(*)
            FROM viewed_events_hourly
            WHERE view_date_hour = '2023-01-01 00:00:00'"""
        )
        == 7
    )


def test_write_to_database_success_assert_idempotency():
    task.write_to_database(date_hour=date_hour, db=db)
    task.write_to_database(date_hour=date_hour, db=db)
    assert (
        db._run_sql_statement_return_rowcount(
            """SELECT COUNT(*)
            FROM viewed_events_hourly
            WHERE view_date_hour = '2023-01-01 00:00:00'"""
        )
        == 7
    )
