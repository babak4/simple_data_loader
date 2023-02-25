from datetime import datetime


def get_data_path(data_root_dir: str, date_hour: datetime) -> str:
    """
    Genarates the absolute path in which the event data
    for a specific hour of a specific date can be found.

    :param data_root_dir: The absolute path of the data directory.
    :param date_hour: The context date/hour for the data.
    """

    if not isinstance(date_hour, datetime):
        raise ValueError('The date_hour parameter should be an instance of datetime!')

    return f"{data_root_dir}/{date_hour.strftime('%Y/%m/%d/%H')}"
