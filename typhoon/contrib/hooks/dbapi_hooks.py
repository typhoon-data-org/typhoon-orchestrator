import sqlite3
from abc import ABC
from typing import Union

import psycopg2 as psycopg2
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.hook_interface import HookInterface


DbApiConnection = Union[SnowflakeConnection, sqlite3.Connection]


class DbApiHook(HookInterface, ABC):
    def __enter__(self) -> DbApiConnection:
        raise NotImplementedError


class PostgresHook(DbApiHook):
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = None

    def __enter__(self) -> psycopg2.extensions.connection:
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

    def __enter__(self) -> SnowflakeConnection:
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

    def __enter__(self) -> sqlite3.Connection:
        conn_params = get_connection_params(self.conn_id)
        self.connection = sqlite3.connect(database=conn_params.extra['database'])
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
