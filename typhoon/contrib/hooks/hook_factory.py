import importlib.util
import os

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.dbapi_hooks import SqliteHook, PostgresHook, SnowflakeHook
from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.settings import typhoon_directory

HOOK_MAPPINGS = {
    'sqlite': SqliteHook,
    'postgres': PostgresHook,
    'snowflake': SnowflakeHook,
}


def get_hook(conn_id: str) -> HookInterface:
    conn_params = get_connection_params(conn_id)
    # TODO: Can be improved, we query the database twice. Once to get the connection params and again in the hook enter
    hook_class = HOOK_MAPPINGS.get(conn_params.conn_type)
    if hook_class:
        return hook_class(conn_id)
    else:
        return get_user_defined_hook(conn_id, conn_params)


def get_user_defined_hook(conn_id: str) -> HookInterface:
    spec = importlib.util.spec_from_file_location(
        'user_hooks',
        os.path.join(typhoon_directory(), 'hooks', 'hook_factory.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.get_hook(conn_id)
