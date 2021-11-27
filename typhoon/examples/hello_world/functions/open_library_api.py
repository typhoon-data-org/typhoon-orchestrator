import datetime
from typing import Optional, List, Union
from typing_extensions import Literal


import requests

ENDPOINT = 'https://openlibrary.org/search/authors.json'
ENDPOINT_WORKS = 'https://openlibrary.org/authors'


def get_author(
        requested_author: Optional[str],
        split_list_into_batches: bool = False
) -> tuple:
    params = {}
    params['q'] = requested_author    
    print(f'Calling endpoint Get Authors'  + ENDPOINT)
    response = requests.get(ENDPOINT, params=params)
    docs = []     
    for doc in response.json()['docs']:
        doc['requested_author'] =  requested_author
        docs.append(doc)
    if split_list_into_batches:
        yield from docs
    else:
        yield docs


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

# from fuzzywuzzy import fuzz
#
# def is_similar_name(requested_author: str, returned_author: str) -> bool:
#     r = fuzz.ratio(requested_author.lower(),returned_author.lower())
#     print('')
#     print (f'Scoring of r  {r}')
#     print('')
#     return True if r > 50 else False



if __name__ == '__main__':
    i = 0 
    for a in get_author('George R. R. Martin', True):
        
        print(a)
        print (i)
        i = i + 1
    