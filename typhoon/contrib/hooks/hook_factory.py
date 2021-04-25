import importlib.util
import inspect

from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.core.metadata_store_interface import MetadataStoreInterface
from typhoon.core.settings import Settings
from typhoon.introspection.introspect_extensions import get_hooks_info


def get_hook(conn_id: str) -> HookInterface:
    metadata_store = Settings.metadata_store()
    conn = metadata_store.get_connection(conn_id)
    hook = get_user_defined_hook(conn_id, metadata_store)
    if hook:
        return hook
    hook_class = get_hooks_info()[conn.conn_type]
    return hook_class(conn.get_connection_params())


def get_user_defined_hook(conn_id: str, metadata_store: MetadataStoreInterface = None) -> HookInterface:
    if metadata_store is None:
        metadata_store = Settings.metadata_store()
    conn = metadata_store.get_connection(conn_id)
    hooks_files = (Settings.typhoon_home / 'hooks').rglob('*.py')
    for hooks_file in hooks_files:
        spec = importlib.util.spec_from_file_location(str(hooks_file).split('.py')[0], str(hooks_file))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for cls_name, cls in inspect.getmembers(module, inspect.isclass):
            conn_type = getattr(cls, 'conn_type', None)
            if conn_type == conn.conn_type:
                return cls(conn.get_connection_params())
