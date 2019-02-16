from io import BytesIO
from typing import Iterable

from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface
from typhoon.contrib.hooks.hook_factory import get_hook


def write_data(data: BytesIO, conn_id: str, path: str) -> Iterable[str]:
    """
    Write the given data to the path specified.
    :param data: Bytes buffer
    :param conn_id: A connection id belonging to a FileSystemHookInterface
    :param path: Path where the data should be written
    :return:
    """
    hook: FileSystemHookInterface = get_hook(conn_id)
    with hook:
        hook.write_data(data, path)
    yield path
