from pathlib import Path
from typing import Optional

from typhoon.connections import Connection
from typhoon.core import settings
from typhoon.core.config import TyphoonConfig
from typhoon.core.metadata_store_interface import MetadataStoreInterface
from typhoon.variables import Variable


class SQLiteMetadataStore(MetadataStoreInterface):
    def __init__(self, config: Optional[TyphoonConfig] = None):
        from sqlitedict import SqliteDict

        self.config = config or TyphoonConfig()
        self.db_path = str(Path(settings.typhoon_home()) / f'{self.config.project_name}.db')
        self.conn_connections = SqliteDict(self.db_path, tablename=self.config.connections_table_name, autocommit=True)
        self.conn_variables = SqliteDict(self.db_path, tablename=self.config.variables_table_name, autocommit=True)

    def close(self):
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
