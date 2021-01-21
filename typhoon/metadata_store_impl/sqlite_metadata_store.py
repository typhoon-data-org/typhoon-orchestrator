from pathlib import Path
from typing import Union, List

from typhoon.connections import Connection
from typhoon.core.dags import DagDeployment
from typhoon.core.metadata_store_interface import MetadataStoreInterface, MetadataObjectNotFound
from typhoon.core.settings import Settings
from typhoon.variables import Variable


class SQLiteMetadataStore(MetadataStoreInterface):
    def __init__(self, db_path: str, no_conns_and_vars=False):
        from sqlitedict import SqliteDict

        self.db_path = db_path
        if not no_conns_and_vars:
            self.conn_connections = SqliteDict(self.db_path, tablename=Settings.connections_table_name)
            self.conn_variables = SqliteDict(self.db_path, tablename=Settings.variables_table_name)
        self.conn_dag_deployments = SqliteDict(self.db_path, tablename=Settings.dag_deployments_table_name)

    def close(self):
        self.conn_connections.close()
        self.conn_variables.close()
        self.conn_dag_deployments.close()

    def exists(self) -> bool:
        return Path(self.db_path).exists()

    def migrate(self):
        open(str(self.db_path), 'a').close()

    def get_connection(self, conn_id: str) -> Connection:
        if conn_id not in self.conn_connections.keys():
            raise MetadataObjectNotFound(f'Connection "{conn_id}" is not set')
        return self.conn_connections[conn_id]

    def get_connections(self, to_dict: bool = False) -> List[Union[dict, Connection]]:
        return [conn.__dict__ if to_dict else conn for conn in self.conn_connections.values()]

    def set_connection(self, conn: Connection):
        self.conn_connections[conn.conn_id] = conn
        self.conn_connections.commit()

    def delete_connection(self, conn: Union[str, Connection]):
        del self.conn_connections[conn.conn_id if isinstance(conn, Connection) else conn]
        self.conn_connections.commit()

    def get_variable(self, variable_id: str) -> Variable:
        if variable_id not in self.conn_variables.keys():
            raise MetadataObjectNotFound(f'Variable "{variable_id}" is not set')
        return self.conn_variables[variable_id]

    def get_variables(self, to_dict: bool = False) -> List[Union[dict, Variable]]:
        return [var.dict_contents() if to_dict else var for var in self.conn_variables.values()]

    def set_variable(self, variable: Variable):
        self.conn_variables[variable.id] = variable
        self.conn_variables.commit()

    def delete_variable(self, variable: Union[str, Variable]):
        del self.conn_variables[variable.id if isinstance(variable, Variable) else variable]
        self.conn_variables.commit()

    def get_dag_deployment(self, deployment_hash: str) -> DagDeployment:
        if deployment_hash not in self.conn_connections.keys():
            raise MetadataObjectNotFound(f'Dag deployment "{deployment_hash}" is not set')
        return self.conn_connections[deployment_hash]

    def get_dag_deployments(self, to_dict: bool = False) -> List[Union[dict, DagDeployment]]:
        return [x.dict() if to_dict else x for x in self.conn_dag_deployments.values()]

    def set_dag_deployment(self, dag_deployment: DagDeployment):
        self.conn_dag_deployments[dag_deployment.deployment_hash] = dag_deployment
        self.conn_dag_deployments.commit()
