from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.connections import ConnectionParams

class ElasticsearchHook(HookInterface):
    conn_type = 'elasticsearch'

    def __init__(self, conn_params: ConnectionParams, conn_type: str = 'client'):
        self.conn_params = conn_params
        self.conn_type = conn_type

    def __enter__(self):
        from elasticsearch import Elasticsearch

        conn_params = self.conn_params
        credentials = {
            'host': conn_params.host,
            'port': conn_params.port
        }
        self.es = Elasticsearch([credentials])
        return self.es

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.es = None