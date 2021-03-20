import importlib.util
import inspect

from typhoon.contrib.hooks.dbapi_hooks import SqliteHook, PostgresHook, SnowflakeHook, EchoDbHook, DuckDbHook, \
    BigQueryHook
from typhoon.contrib.hooks.debug_hooks import EchoHook
from typhoon.contrib.hooks.filesystem_hooks import S3Hook, LocalStorageHook, FTPHook, GCSHook
from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.contrib.hooks.kafka_hooks import KafkaProducerHook, KafkaConsumerHook
from typhoon.contrib.hooks.singer_hook import SingerHook
from typhoon.contrib.hooks.sqlalchemy_hook import SqlAlchemyHook
from typhoon.core.settings import Settings

HOOK_MAPPINGS = {
    'sqlite': SqliteHook,
    'postgres': PostgresHook,
    'snowflake': SnowflakeHook,
    'bigquery': BigQueryHook,
    'echodb': EchoDbHook,
    'duckdb': DuckDbHook,
    's3': S3Hook,
    'gcs': GCSHook,
    'ftp': FTPHook,
    'local_storage': LocalStorageHook,
    'sqlalchemy': SqlAlchemyHook,
    'singer': SingerHook,
    'echo': EchoHook,
    'kafka_producer': KafkaProducerHook,
    'kafka_consumer': KafkaConsumerHook,
}


def get_hook(conn_id: str) -> HookInterface:
    metadata_store = Settings.metadata_store()
    # print(Settings.metadata_db_url)
    conn = metadata_store.get_connection(conn_id)
    hook_class = HOOK_MAPPINGS.get(conn.conn_type)
    if hook_class:
        return hook_class(conn.get_connection_params())
    else:
        return get_user_defined_hook(conn_id)


def get_user_defined_hook(conn_id: str) -> HookInterface:
    metadata_store = Settings.metadata_store()
    conn = metadata_store.get_connection(conn_id)
    hooks_files = (Settings.typhoon_home / 'hooks').rglob('*.py')
    for hooks_file in hooks_files:
        spec = importlib.util.spec_from_file_location(str(hooks_file).split('.py')[0], str(hooks_file))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for cls_name, cls in inspect.getmembers(mod, inspect.isclass):
            conn_type = getattr(cls, 'conn_type', None)
            if conn_type == conn.conn_type:
                return cls(conn.get_connection_params())
