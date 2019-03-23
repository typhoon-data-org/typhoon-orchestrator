from io import BytesIO
from typing import Iterable

from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface


def write_data(data: BytesIO, hook: FileSystemHookInterface, path: str) -> Iterable[str]:
    """
    Write the given data to the path specified.
    :param data: Bytes buffer
    :param hook: A FileSystemHookInterface hook instance
    :param path: Path where the data should be written
    :return:
    """
    with hook:
        hook.write_data(data, path)
    yield path


def list_directory(hook: FileSystemHookInterface, path: str) -> Iterable[str]:
    with hook:
        for x in hook.list_directory(path):
            yield x
