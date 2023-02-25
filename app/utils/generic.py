import logging
import os
import pathlib
from datetime import datetime, timezone

from jinja2 import Template


def get_logger(log_file_path: str) -> logging.Logger:
    logger = logging.getLogger('Data Loader')

    logfile_timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    logfile_name = f'data_loader_{logfile_timestamp_str}.log'
    lof_file_path = os.path.join(log_file_path, logfile_name)
    print(f'Setting up a logfile in {lof_file_path}')
    logging.basicConfig(
        filename=lof_file_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(filename)s:%(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    return logger


def get_rendered_sql_file_contents(
    file_path: str, sql_name: str, parameters: dict = None
) -> str:
    params = parameters or {}

    init_sql_statement_path = pathlib.PurePath(file_path, f'{sql_name}.sql')
    init_sql_statement = pathlib.Path(init_sql_statement_path).read_text()
    return Template(init_sql_statement).render(**params)
