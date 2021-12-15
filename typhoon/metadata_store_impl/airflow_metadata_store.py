import json
from typing import List, Union

from sqlalchemy.orm.exc import UnmappedInstanceError

from typhoon.connections import Connection
from typhoon.core.dags import DagDeployment
from typhoon.core.metadata_store_interface import MetadataStoreInterface, MetadataObjectNotFound
from typhoon.core.settings import Settings
from typhoon.deployment.targets.airflow.airflow_database import set_airflow_db
from typhoon.variables import Variable, VariableType


def typhoon_airflow_variable_name_for_info(name: str) -> str:
    return f'typhoon:info#{name}'


def typhoon_airflow_variable_name_for_value(name: str) -> str:
    return f'typhoon:value#{name}'


def typhoon_airflow_conn_name(name: str) -> str:
    return f'typhoon#{name}'


class AirflowMetadataStore(MetadataStoreInterface):
    """
    Stores all connections and variables in Airflow's database so they can be accessed from it.
    Also creates an SQLite metadata store to keep track of DAG deployments
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.fernet_key = Settings.fernet_key
        self.opened_sqlite_store = False

    @property
    def sqlite_store(self):
        from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
        self.opened_sqlite_store = True
        metadata_store_path = Settings.default_sqlite_path.split(':')[1]
        return SQLiteMetadataStore(metadata_store_path, no_conns_and_vars=True)

    def close(self):
        if self.opened_sqlite_store:
            self.sqlite_store.close()

    def exists(self) -> bool:
        return self.sqlite_store.exists()

    def migrate(self):
        self.sqlite_store.migrate()

    def get_connection(self, conn_id: str) -> Connection:
        af_name = typhoon_airflow_conn_name(conn_id)
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            af_conn = db.get_connection(af_name)
            if af_conn is None:
                raise MetadataObjectNotFound(f'Connection "{conn_id}" is not set')
            conn = Connection(
                conn_id=conn_id,
                conn_type=af_conn.conn_type,
                host=af_conn.host,
                port=af_conn.port,
                login=af_conn.login,
                password=af_conn.password,
                schema=af_conn.schema,
                extra=af_conn.extra_dejson,
            )
        return conn

    def get_connections(self, to_dict: bool = False) -> List[Union[dict, Connection]]:
        result = []
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            for af_conn in db.get_connections():
                if af_conn.conn_id.startswith('typhoon#'):
                    conn = Connection(
                        conn_id=af_conn.conn_id.split('#')[1],
                        conn_type=af_conn.conn_type,
                        host=af_conn.host,
                        port=af_conn.port,
                        login=af_conn.login,
                        password=af_conn.password,
                        schema=af_conn.schema,
                        extra=af_conn.extra_dejson,
                    )
                    if to_dict:
                        conn = conn.__dict__
                    result.append(conn)
        return result

    def set_connection(self, conn: Connection):
        self.delete_connection(conn)
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            db.set_connection(
                conn_id=typhoon_airflow_conn_name(conn.conn_id),
                **conn.get_connection_params().__dict__
            )

    def delete_connection(self, conn: Union[str, Connection]):
        conn_id = conn.conn_id if isinstance(conn, Connection) else conn
        af_name = typhoon_airflow_conn_name(conn_id)
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            try:
                db.delete_connection(af_name)
            except UnmappedInstanceError:
                pass

    def get_variable(self, variable_id: str) -> Variable:
        info_var = typhoon_airflow_variable_name_for_info(variable_id)
        value_var = typhoon_airflow_variable_name_for_value(variable_id)
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            info = db.get_variable(info_var)
            if info is None:
                raise MetadataObjectNotFound(f'Variable "{variable_id}" is not set')
            info = json.loads(info)
            value = db.get_variable(value_var)
        return Variable(id=info['id'], type=VariableType(info['type']), contents=value)

    def get_variables(self, to_dict: bool = False) -> List[Union[dict, Variable]]:
        result = []
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            for af_var in db.get_variables():
                if af_var.key.startswith('typhoon:info#'):
                    info = json.loads(af_var.val)
                    var_id = info['id']
                    value_var = typhoon_airflow_variable_name_for_value(var_id)
                    contents = db.get_variable(value_var)
                    if contents is None:
                        raise MetadataObjectNotFound(f'Variable "{var_id}" does not have a value set')
                    var = Variable(id=var_id, type=VariableType(info['type']), contents=contents)
                    if to_dict:
                        var = var.dict_contents()
                    result.append(var)
        return result

    def set_variable(self, variable: Variable):
        """
        Sets Airflow variables in JSON like:
        {
            "id": "variable_name",
            "type": "variable_type",
            "contents": "contents",
        }
        Following the same schema as the typhoon variable
        """
        info_var = typhoon_airflow_variable_name_for_info(variable.id)
        value_var = typhoon_airflow_variable_name_for_value(variable.id)
        info = variable.dict_contents()
        del info['contents']
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            db.set_variable(info_var, json.dumps(info))
            db.set_variable(value_var, variable.contents)

    def delete_variable(self, variable: Union[str, Variable]):
        var_name = variable.id if isinstance(variable, Variable) else variable
        info_var = typhoon_airflow_variable_name_for_info(var_name)
        value_var = typhoon_airflow_variable_name_for_value(var_name)
        with set_airflow_db(self.db_path, self.fernet_key) as db:
            db.delete_variable(info_var)
            db.delete_variable(value_var)

    def get_dag_deployment(self, deployment_hash: str) -> DagDeployment:
        return self.sqlite_store.get_dag_deployment(deployment_hash)

    def get_dag_deployments(self, to_dict: bool = False) -> List[Union[dict, DagDeployment]]:
        return self.sqlite_store.get_dag_deployments(to_dict)

    def set_dag_deployment(self, dag_deployment: DagDeployment):
        return self.sqlite_store.set_dag_deployment(dag_deployment)
