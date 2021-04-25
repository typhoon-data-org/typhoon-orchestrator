from fs.base import FS
from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface


class Echo:
    def __init__(self, name=None, parent=None):
        self.name = name
        self.parent = parent

    def __call__(self, *args, **kwargs):
        print(f'** Calling {self.name} for {self.parent} with the following arguments')
        for i, arg in enumerate(args):
            print(f'** arg{i} is of type:', type(arg))
            print(f'** arg{i}:', arg)
        for k, v in kwargs.items():
            print(f'**{k}:', v)

    def __getattr__(self, item):
        return Echo(item, parent=self.name)


class EchoHook(FileSystemHookInterface):
    conn_type = 'echo'

    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params

    # noinspection PyTypeChecker
    def __enter__(self) -> FS:
        return Echo('EchoConnection')

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
