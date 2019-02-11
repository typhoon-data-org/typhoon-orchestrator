import os
import re
import unicodedata
from io import BytesIO
from nturl2path import pathname2url
from typing import Iterable

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.aws_hooks import AwsSessionHook
from typhoon.contrib.hooks.hook_interface import HookInterface


class FileSystemHookInterface(HookInterface):

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    def list_directory(self, path: str) -> Iterable[str]:
        raise NotImplementedError

    def write_data(self, data: BytesIO, path: str):
        raise NotImplementedError

    def read_data(self, path: str) -> bytes:
        raise NotImplementedError


class S3Hook(FileSystemHookInterface, AwsSessionHook):
    def __init__(self, conn_id: str):
        AwsSessionHook.__init__(self, conn_id)

    def __enter__(self):
        AwsSessionHook.__enter__(self)
        self.bucket = self.conn_params.extra['bucket']

    def __exit__(self, exc_type, exc_val, exc_tb):
        AwsSessionHook.__exit__(self, exc_type, exc_val, exc_tb)

    def list_directory(self, path: str) -> Iterable[str]:
        s3 = self.session.resource('s3')

        kwargs = {}
        while True:
            response = s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=path,
                **kwargs,
            )
            if 'Contents' not in response.keys():
                break

            yield from (x['Key'] for x in response['Contents'])
            try:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            except KeyError:
                break

    def write_data(self, data: BytesIO, path: str, encrypt=False):
        s3 = self.session.client('s3')

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"

        s3.upload_fileobj(data, self.bucket, path, ExtraArgs=extra_args)

    def read_data(self, path: str) -> bytes:
        s3 = self.session.resource('s3')

        obj = s3.Object(self.bucket, path)
        return obj.get()['Body'].read().decode('utf-8')


class LocalStorageHook(FileSystemHookInterface):
    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def __enter__(self):
        conn_params = get_connection_params(self.conn_id)
        self.base_path = conn_params.extra.get('base_path', '')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.base_path = None

    def _file_path(self, path):
        return os.path.join(self.base_path, self._slugify_path(path))

    @staticmethod
    def _slugify(value: str):
        """
        Normalizes string, converts to lowercase, removes non-alpha characters,
        and converts spaces to hyphens.
        """
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode()
        value = str(re.sub(r'[^.\w\s-]', '', value).strip().lower())
        value = str(re.sub(r'[-\s]+', '-', value))
        return value

    def _slugify_path(self, path: str):
        return '/'.join([self._slugify(x) for x in path.split('/')])

    def list_directory(self, path: str) -> Iterable[str]:
        return os.listdir(self._file_path(path))

    def write_data(self, data: BytesIO, path: str):
        file_path = self._file_path(path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(data.getvalue())

    def read_data(self, path: str) -> bytes:
        with open(self._file_path(path), 'wb') as f:
            return f.read()
