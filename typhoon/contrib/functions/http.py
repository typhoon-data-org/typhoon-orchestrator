import requests
from typing import NamedTuple

from requests import Response

from typhoon.contrib.hooks.http_hooks import BasicAuthHook


class GetResponse(NamedTuple):
    response: Response
    metadata: dict


def get(hook: BasicAuthHook, path: str, metadata: dict = None) -> GetResponse:
    yield GetResponse(
        response=requests.get(hook.url(path), auth=hook.basic_auth_params),
        metadata=metadata or {},
    )


def get_raw(url: str, metadata: dict = None) -> GetResponse:
    """Perform HTTP GET request on URL and return a named tuple with the following structure:
- `response`: requests.Response type from calling requests.get
- `metadata`: If you passed a metadata dictionary it will return it, otherwise it's an empty dict
    """
    yield GetResponse(
        response=requests.get(url),
        metadata=metadata or {},
    )
