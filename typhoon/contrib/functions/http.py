import requests

from typhoon.contrib.hooks.http_hooks import BasicAuthHook


def get(hook: BasicAuthHook, path: str):
    return requests.get(hook.url(path), auth=hook.basic_auth_params)
