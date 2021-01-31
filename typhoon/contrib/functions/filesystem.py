from io import BytesIO
from pathlib import Path
from typing import Iterable, NamedTuple, Union

from fs.copy import copy_fs

from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface


class ReadDataResponse(NamedTuple):
    data: bytes
    path: str


def read_data(hook: FileSystemHookInterface, path: Union[Path, str]) -> ReadDataResponse:
    """
    Reads the data from a file given its relative path and returns a named tuple with the shape (data: bytes, path: str)
    :param hook: FileSystem Hook
    :param path: File path relative to base directory
    """
    with hook as conn:
        print(f'Reading from {path}')
        return ReadDataResponse(data=conn.readbytes(str(path)), path=path)


def write_data(data: Union[str, bytes, BytesIO], hook: FileSystemHookInterface, path: Union[Path, str]) -> Iterable[str]:
    """
    Write the given data to the path specified.
    :param data: Bytes buffer
    :param hook: A FileSystemHookInterface hook instance
    :param path: Path where the data should be written
    """
    if isinstance(data, BytesIO):
        data = BytesIO.getvalue()
    elif isinstance(data, str):
        data = data.encode()
    path = str(path)
    with hook as conn:
        print(f'Writing to {path}')
        conn.writebytes(path, data)
    yield path


def list_directory(hook: FileSystemHookInterface, path: Union[Path, str]) -> Iterable[str]:
    """
    List all the files in a given directory relative to base path
    :param hook: FileSystem Hook
    :param path: Directory relative path
    """
    with hook as conn:
        print(f'Listing directory {path}')
        yield from [str(Path(path) / f) for f in conn.listdir(str(path))]


def copy(source_hook: FileSystemHookInterface, source_path: str, destination_hook: FileSystemHookInterface, destination_path: str):
    """
    Copy a file from a source filesystem to a destination filesystem
    """
    with source_hook as source_conn, destination_hook as dest_conn:
        print(f'Copy {source_path} to {destination_path}')
        copy_fs(source_conn.opendir(source_path), dest_conn.opendir(destination_path))
