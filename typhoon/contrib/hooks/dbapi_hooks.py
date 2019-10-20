from typing import Optional

from typing_extensions import Protocol

from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.hook_interface import HookInterface


class CursorProtocol(Protocol):
    def execute(self, query: str): ...

    def fetchmany(self, size): ...

    def fetchall(self): ...

    def close(self): ...


class DbApiConnection(Protocol):
    def cursor(self) -> CursorProtocol: ...

    def close(self): ...


class DbApiHook(HookInterface, Protocol):
    connection: Optional[DbApiConnection]

    def __init__(self, conn_params: ConnectionParams): ...
    
    def __enter__(self) -> DbApiConnection:
        raise NotImplementedError


class PostgresHook(DbApiHook):
    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.connection = None

    def __enter__(self) -> DbApiConnection:
        import psycopg2

        credentials = {
            'host': self.conn_params.host,
            'user': self.conn_params.login,
            'password': self.conn_params.password,
            'dbname': self.conn_params.extra.get('dbname'),
            'port': self.conn_params.port
        }
        self.connection = psycopg2.connect(**credentials)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self.connection = None


class SnowflakeHook(DbApiHook):
    def __init__(self, conn_params):
        self.conn_params = conn_params

    # noinspection PyProtectedMember
    def __enter__(self) -> DbApiConnection:
        import snowflake.connector

        conn_params = self.conn_params
        credentials = {
            'account': conn_params.extra['account'],
            'region': conn_params.extra['region'],
            'user': conn_params.login,
            'password': conn_params.password,
            'database': conn_params.extra['database'],
            'schema': conn_params.schema,
            'warehouse': conn_params.extra['warehouse'],
            'role': conn_params.extra['role'],
        }
        self.connection = snowflake.connector.connect(**credentials)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


class SqliteHook(DbApiHook):
    def __init__(self, conn_params):
        self.conn_params = conn_params

    def __enter__(self) -> DbApiConnection:
        import sqlite3

        self.connection = sqlite3.connect(database=self.conn_params.extra['database'])
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
