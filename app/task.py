import logging
import pathlib
from datetime import date, datetime

import click
from db_utils.db_factory import DB, DBFactory
from utils import generic
from utils.data_utils import get_data_path

CURRENT_DIR = pathlib.Path().resolve()
DATA_PATH = f'{CURRENT_DIR}/data'
LOG_PATH = f'{CURRENT_DIR}/logs'


def num_of_event_views_per_hour(date_hour: datetime, data_path: str, db: DB) -> int:
    """
    Extracts the data for a specific hour of of a specific date
    from the generic DATA_PATH and stores it in a temporary staging
    DuckDB table called tmp_hourly_aggregates.

    :param date_hour: The context date/hour for the data.
    :param data_path: The absolute path in which the data for
        the hour can be found.
    :param conn: The connection object to a DuckDB database.
    """

    logging.info(f'Extracting records for {date_hour}')
    date_hour_dir = get_data_path(data_path, date_hour)
    affected_records = db.stage_aggregate_view_events(date_hour_dir)
    logging.info(f'Number of extracted records for {date_hour}: {affected_records}')


def write_to_database(date_hour: datetime, db: DB) -> None:
    """
    [Re-]populates the `viewed_events_hourly` table with
    the data for a specific hour of of a specific date.

    :param date_hour: The context date/hour for the data.
    :param conn: The connection object to a DuckDB database.
    """

    affected_records = db.clear_aggregate_view_events(date_hour)
    logging.info(f'Deleted {affected_records} records for {date_hour}')

    affected_records = db.load_aggregate_view_events_from_staging(date_hour)
    logging.info(
        'Number of inserted records in viewed_events_hourly'
        f' for {date_hour}: {affected_records}\n'
    )


@click.command()
@click.option(
    '--day', type=click.DateTime(formats=['%Y-%m-%d']), default=str(date.today())
)
def main(day: date) -> None:
    """
    Generate the number of event views per hour per source for a given day
    and write the results to a database.

    :param day: The day to generate the data for.
    """

    logger = generic.get_logger(LOG_PATH)
    logger.info(f'Staring the data load for {day}\n')

    db = DBFactory().get_db('DuckDB', 'dwh.db')

    for hour in range(24):
        date_hour = datetime.combine(day, datetime.min.time()).replace(hour=hour)
        num_of_event_views_per_hour(date_hour=date_hour, data_path=DATA_PATH, db=db)

        write_to_database(date_hour=date_hour, db=db)

    logger.info(f'Finished loading the data for {day}')


if __name__ == '__main__':
    main()
