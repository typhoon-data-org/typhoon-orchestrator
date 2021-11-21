from io import BytesIO
from pathlib import Path
from typing import Iterable, NamedTuple, Union, List, Optional

from fs.copy import copy_fs
from fs.info import Info
from typing_extensions import Literal

from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface


class ReadDataResponse(NamedTuple):
    data: bytes
    info: Info


class WriteDataResponse(NamedTuple):
    metadata: dict
    path: str


def read_data(hook: FileSystemHookInterface, path: Union[Path, str]) -> ReadDataResponse:
    """
    Reads the data from a file given its relative path and returns a named tuple with the shape (data: bytes, path: str)
    :param hook: FileSystem Hook
    :param path: File path relative to base directory
    """
    with hook as conn:
        print(f'Reading from {path}')
        return ReadDataResponse(data=conn.readbytes(str(path)), info=conn.getinfo(path))


def write_data(
        data: Union[str, bytes, BytesIO],
        hook: FileSystemHookInterface,
        path: Union[Path, str],
        create_intermediate_dirs: bool = False,
        metadata: Optional[dict] = None,
        return_path_format: Literal['relative', 'absolute', 'url'] = 'relative',
) -> Iterable[str]:
    """
    Write the given data to the path specified.
    :param metadata: optional dict
    :param data: Bytes buffer
    :param hook: A FileSystemHookInterface hook instance
    :param path: Path where the data should be written
    :param create_intermediate_dirs: Create intermediate directories if necessary
    :param return_path_format: relative, absolute or url
    """
    if isinstance(data, BytesIO):
        data = data.getvalue()
    elif isinstance(data, str):
        data = data.encode()
    path = str(path)
    with hook as conn:
        if create_intermediate_dirs:
            print('Creating intermediate directories')
            conn.makedirs(str(Path(path).parent), recreate=True)
        print(f'Writing to {path}')
        conn.writebytes(path, data)
        if return_path_format == 'relative':
            return_path = path
        elif return_path_format == 'absolute':
            return_path = str(conn.open(path))
        elif return_path_format == 'url':
            return_path = conn.geturl(path)
        else:
            raise ValueError(f'return_path_format should be "relative", "absolute" or "url". Found "{return_path_format}"')
    yield WriteDataResponse(metadata=metadata, path=return_path)


def list_directory(hook: FileSystemHookInterface, path: Union[Path, str] = '/') -> Iterable[str]:
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


def glob(hook: FileSystemHookInterface, pattern: str, path: Union[Path, str] = '/', exclude_dirs: Optional[List[str]] = None) -> Iterable[Info]:
    """
    List all the files in a given directory matching the glob pattern
    :param hook: Filesystem hook
    :param pattern: Glob pattern e.g. '*.csv' or '"**/*.py"'
    :param path: Optional directory path
    :param exclude_dirs: An optional list of patterns to exclude when searching e.g. ["*.git"]
    :return fs.info.Info
        path: str - A string with the matched file
        raw_info (dict) – A dict containing resource info.
            accessed: datetime. The resource last access time, or None.
            copy(to_datetime=None)[source]: Create a copy of this resource info object.
            created: datetime. The resource creation time, or None.
            get(namespace, key, default=None)[source]: Get a raw info value.
                Example
                >>> info.get('access', 'permissions')
                ['u_r', 'u_w', '_wx']
            gid: int. The group id of the resource, or None.
            group: str. The group of the resource owner, or None.
            has_namespace(namespace)[source]: Check if the resource info contains a given namespace.
            is_dir: bool. True if the resource references a directory.
            is_link: bool. True if the resource is a symlink.
            is_writeable(namespace, key)[source]: Check if a given key in a namespace is writable.
            make_path(dir_path)[source]: Make a path by joining dir_path with the resource name.
            modified: datetime. The resource last modification time, or None.
            name: str. The resource name.
            permissions: Permissions. The permissions of the resource, or None.
            size: int. The size of the resource, in bytes.
            stem: str. The name minus any suffixes.
                Example
                >>> info
                <info 'foo.tar.gz'>
                >>> info.stem
                'foo'
            suffix: str. The last component of the name (including dot), or an empty string if there is no suffix.
            suffixes: List. A list of any suffixes in the name.
            target: str. The link target (if resource is a symlink), or None.
            type: ResourceType. The type of the resource.
            uid: int. The user id of the resource, or None.
            user: str. The owner of the resource, or None.
    """
    with hook as conn:
        for match in conn.glob(pattern, path=path, exclude_dirs=exclude_dirs, namespaces=['details', 'access']):
            yield match
