from pathlib import Path
from typing import Optional

from typhoon.config import TyphoonConfig
from typhoon.connections import Connection
from typhoon.core import settings
from typhoon.core.metadata_store_interface import MetadataStoreInterface, MetadataStoreNotInitializedError
from typhoon.variables import Variable


class SQLiteMetadataStore(MetadataStoreInterface):
    def __init__(self, config: Optional[TyphoonConfig] = None):
        self.config = config or TyphoonConfig()
        self._conn_connections = None
        self._conn_variables = None
        self.db_path = str(Path(settings.typhoon_directory()) / f'{self.config.project_name}.db')

    @property
    def conn_connections(self):
        if self._conn_connections is None:
            raise MetadataStoreNotInitializedError(
                'Uninitialized connection for SQLiteMetadataStore please use in a with block')
        return self._conn_connections

    @property
    def conn_variables(self):
        if self._conn_variables is None:
            raise ValueError('Uninitialized connection for SQLiteMetadataStore please use in a with block')
        return self._conn_variables

    def __enter__(self):
        from sqlitedict import SqliteDict

        self._conn_connections = SqliteDict(self.db_path, tablename='connections', autocommit=True)
        self._conn_variables = SqliteDict(self.db_path, tablename='variables', autocommit=True)

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn_connections.close()
        self.conn_variables.close()

    def migrate(self, config: TyphoonConfig):
        pass

    def get_connection(self, conn_id: str) -> Connection:
        return self.conn_connections[conn_id]

    def set_connection(self, conn: Connection):
        self.conn_connections[conn.conn_id] = conn

    def get_variable(self, variable_id: str) -> Variable:
        return self.conn_variables[variable_id]

    def set_variable(self, variable: Variable):
        self.conn_variables[variable.id] = variable
