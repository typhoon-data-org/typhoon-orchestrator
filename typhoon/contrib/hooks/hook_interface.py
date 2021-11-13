from typing_extensions import Protocol, runtime_checkable

from typhoon.connections import ConnectionParams


@runtime_checkable
class HookInterface(Protocol):
    conn_type: str
    conn_params: ConnectionParams

    def __init__(self, conn_params: ConnectionParams): ...
    def __enter__(self): ...
    def __exit__(self, exc_type, exc_val, exc_tb): ...
