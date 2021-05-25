import requests
from typing import NamedTuple

from requests import Response

from typhoon.contrib.hooks.http_hooks import BasicAuthHook


class GetResponse(NamedTuple):
    response: Response
    metadata: dict


def get(hook: BasicAuthHook, path: str, metadata: dict = None) -> GetResponse:
    return GetResponse(
        response=requests.get(hook.url(path), auth=hook.basic_auth_params),
        metadata=metadata or {},
    )
