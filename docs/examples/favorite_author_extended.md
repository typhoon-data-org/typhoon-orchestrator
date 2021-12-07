# Favorite Author - extended version

The front page example 'favorite author' can be extended to illustrate some more advanced concepts:

- Splitting list into batches for individual processing
- Custom Functions (e.g. API calls)
- Custom Transformations
- If Then logic
- Filtering [JMESPath](https://jmespath.org/)
- 2 apis, in chain

We will not go line by line but to call out several specific points.

Firstly here is the DAG graph (Airflow compiled) which explains the structure well:


Here is the YAML it was compiled from:

```yaml
name: favorite_authors_extended_full
schedule_interval: rate(1 day)

Description: >
  A more complex DAG:
    - Splitting list into batches for individual processing
    - Custom Functions (e.g. API calls)
    - Custom Transformations
    - If Then logic
    - Filtering JMESPath (JSON filtering / search)
    - 2 apis, in chain


tasks:
  choose_favorites:
    function: typhoon.flow_control.branch
    args:
      branches:
        - J. K. Rowling
        - George R. R. Martin
        - James Clavell

  get_author:
    input: choose_favorites
    function: functions.open_library_api.get_author
    args:
      requested_author: !Py $BATCH
      split_list_into_batches: true


  is_fuzzy_author:
    input: get_author
    component: typhoon.ifthen
    args:
      condition: !Py |
        lambda x: transformations.author_cleaning.is_similar_name(
          x['name'], 
          x['requested_author'],
          96
        )
      data: !Py $BATCH


  print_match:
    input: is_fuzzy_author.ifthen
    function: typhoon.debug.echo
    args:
      data: !Py f"ACCEPTED --  {$BATCH['name']}."

  print_not_matched:
    input: is_fuzzy_author.ifelse
    function: typhoon.debug.echo
    args:
      data: !Py f"DISCARDED -- {$BATCH['name']} is not similar enough."


  select_keys_valid_author_json:
    input: is_fuzzy_author.ifthen
    function: typhoon.json.search
    args:
      expression: "{key:key, name:name, birth_date: birth_date, work_count: work_count, top_work: top_work, requested_author:requested_author }"
      data: !Py $BATCH

  write_valid_author_json:
    input: select_keys_valid_author_json
    function: typhoon.filesystem.write_data    
    args:
      hook: !Hook data_lake
      data:  !Py transformations.author_cleaning.json_dumps($BATCH)
      path: !MultiStep 
        - !Py $BATCH['key']
        - !Py f'/valid_authors/{$1}.json'
      create_intermediate_dirs: True


  write_error_path_author_json:
    input: is_fuzzy_author.ifelse
    function: typhoon.filesystem.write_data    
    args:
      hook: !Hook data_lake
      data:  !Py transformations.author_cleaning.json_dumps($BATCH)
      path: !MultiStep 
        - !Py $BATCH['key']
        - !Py f'/error_authors/{$1}.json'
      create_intermediate_dirs: True

  get_works:
    input: select_keys_valid_author_json
    function: functions.open_library_api.get_works
    args:
      limit: 50
      author_key: !Py $BATCH['key']

  write_works_json:
      input: get_works
      function: typhoon.filesystem.write_data
      args:
        hook: !Hook data_lake
        data:  !MultiStep
          - !Py $BATCH['entries']
          - !Py typhoon.data.json_array_to_json_records($1)
        path: !MultiStep 
          - !Py $BATCH['links']['author'].replace('/authors/', '')
          - !Py f'/works/{$1}.json'
        create_intermediate_dirs: True


```

## Custom functions to call APIs

These two functions are used to call the Open Library APIs. This is the only complex piece of python code.  

```python
from typing import Optional, List, Union
from typing_extensions import Literal

import requests

ENDPOINT = 'https://openlibrary.org/search/authors.json'
ENDPOINT_WORKS = 'https://openlibrary.org/authors'


def get_author(
        requested_author: str,
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
```

You see we call this in the YAML with the simple:

```yaml
  get_author:
    input: choose_favorites
    function: functions.open_library_api.get_author
    args:
      requested_author: !Py $BATCH
      split_list_into_batches: true
```

## Splitting list of JSON into batches in a custom function

In this case the API we use, Open Library, returns a list of many JSON objects.

We are requesting for example "J. K. Rowling" and the API returns fuzzy matches or related authors. 

```JSON
[
  {
    "name": J. K. Rowling,
    ...
  },
  {
    "name": J N Rowling,
    ...
  },
  {
    "name": J. J. Rowlings,
    ...
  }
```
We want to apply a lambda function against each of these so we must process each row individually. To do this we split the list into individual batches by using 'yield from' in the api function:

```python
def get_author(
        requested_author: str,
        split_list_into_batches: bool = False
) -> tuple:
    
    ...
    
    if split_list_into_batches:
        yield from docs
    else:
        yield docs
```

This allows us to use the arg `split_list_into_batches: true` in the YAML. This will then return each of the items in the list as a separate `$BATCH`.

## If Then logic 

This DAG uses some If Then logic to handle cleaning and validation. This is not a good example of cleaning but a trivial one to show some possible structure. 

We are using a transformation function to check if the name requested was similar and if not, to discard the row. 

In this case the If Then logic is based on the outcome of the is_similar_name transformation python. We use the outcome twice: once for a set of debug print statements and then also to pass data to downstream steps. 

```yaml
  is_fuzzy_author:
    input: get_author
    component: typhoon.ifthen
    args:
      condition: !Py |
        lambda x: transformations.author_cleaning.is_similar_name(
          x['name'], 
          x['requested_author'],
          96
        )
      data: !Py $BATCH


  print_match:
    input: is_fuzzy_author.ifthen
    function: typhoon.debug.echo
    args:
      data: !Py f"ACCEPTED --  {$BATCH['name']}."

  print_not_matched:
    input: is_fuzzy_author.ifelse
    function: typhoon.debug.echo
    args:
      data: !Py f"DISCARDED -- {$BATCH['name']} is not similar enough."
```

## Filtering JSON with JMESPath 

A great library that we have included is JMESPath which you can use to reduce and transform JSON data easily.

```yaml
  select_keys_valid_author_json:
    input: is_fuzzy_author.ifthen
    function: typhoon.json.search
    args:
      expression: "{key:key, name:name, birth_date: birth_date, work_count: work_count, top_work: top_work, requested_author:requested_author }"
      data: !Py $BATCH
```

This expression above reduces the JSON to only the selected fields. You can explore this more on [JMESPath](https://jmespath.org/).
