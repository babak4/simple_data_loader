import os
from datetime import date, datetime

from app.utils.generic import get_rendered_sql_file_contents

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DATA_LOCATION = f'{CURRENT_DIR}/../sql'
SAMPLE_DATE = date(2023, 1, 1)


def test_get_rendered_sql_file_contents_success_no_template():
    expected_result = """CREATE TABLE IF NOT EXISTS viewed_events_hourly(
  view_date_hour    TIMESTAMP,
  event_name        VARCHAR,
  view_source       VARCHAR,
  viewers_total     INTEGER,
  etl_staging_time  TIMESTAMP,
  etl_load_time     TIMESTAMP
)"""
    assert (
        get_rendered_sql_file_contents(
            file_path=SQL_DATA_LOCATION, sql_name='sql_no_jinja'
        )
        == expected_result
    )


def test_get_rendered_sql_file_contents_success_jinja_template():
    date_hour = datetime.combine(SAMPLE_DATE, datetime.min.time()).replace(hour=0)

    expected_result = """DELETE FROM viewed_events_hourly
WHERE view_date_hour = '2023-01-01 00:00:00'"""
    assert (
        get_rendered_sql_file_contents(
            file_path=SQL_DATA_LOCATION,
            sql_name='sql_jinja',
            parameters={'date_hour': date_hour},
        )
        == expected_result
    )
