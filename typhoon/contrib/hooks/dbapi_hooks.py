from abc import ABC

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.hook_interface import HookInterface
from type_extensions import Protocol


class CursorProtocol(Protocol):
    def execute(self, query: str):
        ...

    def fetchmany(self, size):
        ...

    def fetchall(self):
        ...

    def close(self):
        ...


class DbApiConnection(Protocol):
    def cursor(self) -> CursorProtocol:
        ...

    def close(self):
        ...


class DbApiHook(HookInterface, Protocol):
    @property
    def connection(self) -> DbApiConnection:
        ...
    
    def __enter__(self):
        raise NotImplementedError


class PostgresHook(DbApiHook):
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = None

    def __enter__(self):
        import psycopg2

        self.conn_params = get_connection_params(self.conn_id)
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
    def __init__(self, conn_id):
        self.conn_id = conn_id

    # noinspection PyProtectedMember
    def __enter__(self):
        import snowflake.connector

        conn_params = get_connection_params(self.conn_id)
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
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def __enter__(self):
        import sqlite3

        conn_params = get_connection_params(self.conn_id)
        self.connection = sqlite3.connect(database=conn_params.extra['database'])
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
