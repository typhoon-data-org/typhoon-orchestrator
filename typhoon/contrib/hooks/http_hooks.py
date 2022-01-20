import requests
from typing import Tuple

from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.hook_interface import HookInterface


class BasicAuthHook(HookInterface):
    conn_type = 'basic_auth'

    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params
        self.session = None

    def __enter__(self):
        self.session = requests.Session()
        self.session.auth = self.basic_auth_params

        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
         self.session.close()

    def url(self, path: str):
        return f'{self.conn_params.extra["base_url"].rstrip("/")}/{path.lstrip("/")}'

    @property
    def basic_auth_params(self) -> Tuple[str, str]:
        return self.conn_params.login, self.conn_params.password
