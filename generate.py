import json
import os
import random
import shutil
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import click

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_OUT_PATH = f'{ROOT_DIR}/data'

TRACKING_SOURCES = {'android', 'web', 'ios'}
SESSION_PROPERTY_VALUES = {
    'event_viewed',
    'event_liked',
    'event_shared',
    'event_disliked',
}
EVENT_NAMES = {
    'Bicep: Alexexndra Palace',
    'Fred Again: O2 Academy Brixton',
    'Boiler Room Open Air: Bristol',
}


@dataclass
class SessionProperties:
    type: str
    recieved_at: str
    event_name: str


@dataclass
class Session:
    id: str
    ip: str
    source: str
    user_id: int
    properties: SessionProperties


def random_ip() -> list[str]:
    return '.'.join([str(random.randint(0, 255)) for _ in range(4)])


def random_source() -> str:
    return random.choice(list(TRACKING_SOURCES))


def random_event_property_type() -> str:
    return random.choice(list(SESSION_PROPERTY_VALUES))


def random_id() -> str:
    return str(uuid4())


def random_event() -> str:
    return random.choice(list(EVENT_NAMES))


def random_session_properties(recieved_at: datetime) -> SessionProperties:
    return SessionProperties(
        type=random_event_property_type(),
        recieved_at=recieved_at.isoformat(),
        event_name=random_event(),
    )


def write_nd_out(file_name: str, data: list[Session]) -> None:
    p = Path(os.path.dirname(file_name))
    os.makedirs(p, exist_ok=True)
    with open(file_name, 'w') as f:
        for session in data:
            f.write(json.dumps(session))
            f.write('\n')


def generate_hour(day_to_generate: date, hour: int, hourly_path: str):
    shutil.rmtree(hourly_path, ignore_errors=True)
    random_number_of_files = random.randint(1, 10)
    for _ in range(random_number_of_files):
        random_number_of_sessions = random.randint(1, 100)
        sessions = []
        for _ in range(random_number_of_sessions):
            random_session = Session(
                id=random_id(),
                ip=random_ip(),
                source=random_source(),
                user_id=random_id(),
                properties=random_session_properties(
                    datetime(
                        day_to_generate.year,
                        day_to_generate.month,
                        day_to_generate.day,
                        hour,
                        random.randint(0, 59),
                        random.randint(0, 59),
                    )
                ),
            )
            sessions.append(asdict(random_session))
        write_nd_out(f'{hourly_path}/{random_id()}.json', sessions)


def _root_path(day_to_generate: date) -> str:
    return f"{DATA_OUT_PATH}/{day_to_generate.strftime('%Y/%m/%d')}"


@click.command()
@click.option(
    '--day', type=click.DateTime(formats=['%Y-%m-%d']), default=str(date.today())
)
def main(day: datetime):
    for hour in range(0, 24):
        hourly_path = f'{_root_path(day.date())}/{hour:02}'
        generate_hour(day.date(), hour, hourly_path)


if __name__ == '__main__':
    main()
