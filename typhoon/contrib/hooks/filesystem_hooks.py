from fs.base import FS
from fs.ftpfs import FTPFS
from fs.osfs import OSFS
from fs_s3fs import S3FS
from typing_extensions import Protocol

from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.aws_hooks import AwsSessionHook
from typhoon.contrib.hooks.hook_interface import HookInterface


class FileSystemHookInterface(HookInterface, Protocol):
    conn: FS

    def __enter__(self) -> FS:
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class S3Hook(FileSystemHookInterface, AwsSessionHook):
    def __init__(self, conn_params: ConnectionParams):
        AwsSessionHook.__init__(self, conn_params)

    def __enter__(self) -> S3FS:
        AwsSessionHook.__enter__(self)
        self.bucket = self.conn_params.extra['bucket']
        self.base_path = self.conn_params.extra.get('base_path')
        if self.conn_params.login and self.conn_params.password:
            kwargs = {'aws_access_key_id': self.conn_params.login, 'aws_secret_access_key': self.conn_params.password}
        elif self.session:
            # Get session token
            client = self.session.client('sts')
            session_token = client.get_session_token()
            kwargs = {'aws_session_token': session_token}
        else:
            kwargs = {}
        self.conn = S3FS(self.bucket, dir_path=self.base_path, **kwargs)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        AwsSessionHook.__exit__(self, exc_type, exc_val, exc_tb)
        self.conn.close()
        self.conn = None


class LocalStorageHook(FileSystemHookInterface):
    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params

    def __enter__(self) -> OSFS:
        self.base_path = self.conn_params.extra.get('base_path', '')
        self.conn = OSFS(root_path=self.base_path, create=self.conn_params.extra.get('create', False))
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.base_path = None
        self.conn.close()
        self.conn = None


class FTPHook(FileSystemHookInterface):
    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params

    def __enter__(self) -> FTPFS:
        self.base_path = self.conn_params.extra.get('base_path', '')
        self.conn = FTPFS(
            host=self.conn_params.host,
            user=self.conn_params.login,
            passwd=self.conn_params.password,
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.conn = None
