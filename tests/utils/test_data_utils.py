import os
from datetime import datetime

import pytest
from utils.data_utils import get_data_path

CURRENT_DIR = os.path.curdir


def test_get_data_path_success():
    test_date_hour = datetime(year=2023, month=2, day=7, hour=13)
    assert get_data_path(CURRENT_DIR, test_date_hour) == f'{CURRENT_DIR}/2023/02/07/13'


def test_get_data_path_fail_date_hour_type():
    test_date_hour = object()

    with pytest.raises(ValueError):
        _ = get_data_path(CURRENT_DIR, test_date_hour)
