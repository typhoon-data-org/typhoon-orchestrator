from io import BytesIO

from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface
from typhoon.contrib.hooks.hook_factory import get_hook


def write_data(data: BytesIO, conn_id: str, path: str):
    hook: FileSystemHookInterface = get_hook(conn_id)
    hook.write_data(data, path)
