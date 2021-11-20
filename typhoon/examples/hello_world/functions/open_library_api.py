import datetime
from typing import Optional, List, Union
from typing_extensions import Literal

import requests

ENDPOINT = 'https://openlibrary.org/search/authors.json'
ENDPOINT_WORKS = 'https://openlibrary.org/authors'


def get_author(
        requested_author: Optional[str] = None,
) -> list(dict, str):
    params = {}
    params['q'] = requested_author
    print(f'Calling endpoint Get Authors'  + ENDPOINT)
    response = requests.get(ENDPOINT, params=params)
    yield response.json(), requested_author


def get_works(
        author_key: Optional[str] = None,
        limit: int = 50,
) -> dict:
    params = {
        'limit': limit
    }
    full_endpoint = f'{ENDPOINT_WORKS}/'+ author_key + '/works.json'

    print(f'Calling endpoint {full_endpoint} for author key ' + author_key)
    response = requests.get(full_endpoint, params=params)
    yield response.json()


if __name__ == '__main__':
    for a in get_author('J.K.Rowling'):
        print(a['docs'][0]['key'])