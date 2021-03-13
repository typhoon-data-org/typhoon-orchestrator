import datetime
from typing import Optional, List, Union

import requests

ENDPOINT = 'https://api.exchangeratesapi.io'


def get(
        date: Union[datetime.date, datetime.datetime, None] = None,
        base: Optional[str] = None,
        symbols: Optional[List[str]] = None,
) -> dict:
    if isinstance(date, datetime.datetime):
        date = date.date()
    if isinstance(date, datetime.date):
        date = date.isoformat()
    elif date is None:
        date = 'latest'

    params = {}
    if base:
        params['base'] = base
    if symbols:
        params['symbols'] = symbols
    full_endpoint = f'{ENDPOINT}/{date}'
    print(f'Calling endpoint {full_endpoint}')
    response = requests.get(full_endpoint, params=params)
    return response.json()


def get_history(
        start_at: Union[datetime.date, datetime.datetime],
        end_at: Union[datetime.date, datetime.datetime],
        base: Optional[str] = None,
        symbols: Optional[List[str]] = None,
) -> dict:
    if isinstance(start_at, datetime.datetime):
        start_at = start_at.date()
    if isinstance(start_at, datetime.date):
        start_at = start_at.isoformat()
    if isinstance(end_at, datetime.datetime):
        end_at = end_at.date()
    if isinstance(end_at, datetime.date):
        end_at = end_at.isoformat()

    params = {
        'start_at': start_at,
        'end_at': end_at,
    }
    full_endpoint = f'{ENDPOINT}/history'
    print(f'Calling endpoint {full_endpoint} for dates between {start_at}, {end_at}')
    if base:
        params['base'] = base
    if symbols:
        params['symbols'] = symbols
    response = requests.get(full_endpoint, params=params)
    return response.json()
