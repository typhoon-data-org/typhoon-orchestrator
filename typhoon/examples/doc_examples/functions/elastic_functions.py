from elasticsearch import Elasticsearch
from typing import Optional, List

from hooks.elastic import ElasticsearchHook

# .search(index="sw", body={"query": {"match": {'name':'Darth Vader'}}})

def search(
        hook: ElasticsearchHook,
        index: str,
        body: Optional[str] = None,
):
    with hook as es:
        print('ES SEARCH LOG', index, body)
        return es.search(index=index, body=body)

