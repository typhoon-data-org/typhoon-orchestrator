import importlib.util
import os

from typhoon.contrib.hooks.dbapi_hooks import SqliteHook, PostgresHook, SnowflakeHook
from typhoon.contrib.hooks.filesystem_hooks import S3Hook, LocalStorageHook
from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.contrib.hooks.sqlalchemy_hook import SqlAlchemyHook
from typhoon.core.config import TyphoonConfig
from typhoon.core.settings import typhoon_home

HOOK_MAPPINGS = {
    'sqlite': SqliteHook,
    'postgres': PostgresHook,
    'snowflake': SnowflakeHook,
    's3': S3Hook,
    'local_storage': LocalStorageHook,
    'sqlalchemy': SqlAlchemyHook,
}


def get_hook(conn_id: str) -> HookInterface:
    conn = TyphoonConfig().metadata_store.get_connection(conn_id)
    hook_class = HOOK_MAPPINGS.get(conn.conn_type)
    if hook_class:
        return hook_class(conn.get_connection_params())
    else:
        return get_user_defined_hook(conn_id)


def get_user_defined_hook(conn_id: str) -> HookInterface:
    spec = importlib.util.spec_from_file_location(
        'user_hooks',
        os.path.join(typhoon_home(), 'hooks', 'hook_factory.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.get_hook(conn_id)
