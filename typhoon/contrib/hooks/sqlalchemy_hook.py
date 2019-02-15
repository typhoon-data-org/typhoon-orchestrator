import jinja2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.hook_interface import HookInterface

URL_TEMPLATE = """\
{{dialect}}{% if driver %}+{{driver}}{% endif %}://{{username or ''}}\
{% if password %}:{{password or ''}}{% endif %}{% if host %}@{{host}}{% endif %}{% if port %}:{{port}}{% endif %}/{{database}}\
"""


class SqlAlchemyHook(HookInterface):
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def __enter__(self) -> Engine:
        self.conn_params = get_connection_params(self.conn_id)
        url = jinja2.Template(URL_TEMPLATE).render(dict(
            dialect=self.conn_params.extra['dialect'],
            driver=self.conn_params.extra.get('driver'),
            username=self.conn_params.login,
            password=self.conn_params.password,
            host=self.conn_params.host,
            port=self.conn_params.port,
            database=self.conn_params.extra.get('database'),
        ))
        self.engine = create_engine(url)
        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn_params = None
        self.engine = None
