import jinja2

from typhoon.contrib.hooks.hook_interface import HookInterface

URL_TEMPLATE = """\
{{dialect}}{% if driver %}+{{driver}}{% endif %}://{{username or ''}}\
{% if password %}:{{password or ''}}{% endif %}{% if host %}@{{host}}{% endif %}{% if port %}:{{port}}{% endif %}/{{database}}\
"""


class SqlAlchemyHook(HookInterface):
    conn_type = 'sqlalchemy'

    def __init__(self, conn_params):
        self.conn_params = conn_params

    def __enter__(self):
        from sqlalchemy import create_engine

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
        self.engine = None
