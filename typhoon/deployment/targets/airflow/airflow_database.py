import contextlib
import json
import os
import tempfile
from typing import Optional, Union, ContextManager, List

from airflow import models
from airflow import settings
from airflow.models import Connection, Variable, DagRun
from airflow.utils.db import initdb
from airflow.utils.db import merge_conn
from cryptography.fernet import Fernet
from sqlalchemy import asc


@contextlib.contextmanager
def set_env(**environ):
    """
    Temporarily set the process environment variables.
    :param environ: Environment variables to set
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


class AirflowDb:
    sql_alchemy_conn: str = None

    def __init__(self, sql_alchemy_conn: str):
        self.sql_alchemy_conn = sql_alchemy_conn

    def set_connection(
            self,
            conn_id: str,
            conn_type: str,
            host: Optional[str] = None,
            schema: Optional[str] = None,
            login: Optional[str] = None,
            password: Optional[str] = None,
            port: Optional[int] = None,
            extra: Optional[Union[str, dict]] = None,
    ):
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            new_conn = Connection(conn_id=conn_id, conn_type=conn_type, host=host,
                                login=login, password=password, schema=schema, port=port)
            if extra is not None:
                new_conn.set_extra(extra if isinstance(extra, str) else json.dumps(extra))

            session.add(new_conn)
            session.commit()

    def get_connection(self, conn_id: str) -> Connection:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            return conn

    def delete_connection(self, conn_id: str):
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            session.delete(conn)
            session.commit()

    def set_variable(
            self,
            var_id: str,
            value: str,
            is_encrypted: Optional[bool] = None
    ):
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            new_var = Variable(key=var_id, _val=value, is_encrypted=is_encrypted)
            session.add(new_var)
            session.commit()

    def get_variable(self, var_id: str) -> Variable:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            not_found = object()
            var = Variable.get(var_id, default_var=not_found, session=session)
            return var if var is not not_found else None

    def delete_variable(self, var_id: str):
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            var = session.query(Variable).filter(Variable.key == var_id).first()
            session.delete(var)
            session.commit()

    def list_connections(self) -> List[str]:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            return [x.conn_id for x in session.query(Connection)]

    def get_connections(self) -> List[Connection]:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            return [x for x in session.query(Connection)]

    def get_variables(self) -> List[Variable]:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            return [x for x in session.query(Variable)]

    def get_first_dag_run(self, dag_id) -> Optional[DagRun]:
        assert str(settings.engine.url) == self.sql_alchemy_conn
        with settings.Session() as session:
            dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(asc(DagRun.execution_date)).first()
            return dag_run


@contextlib.contextmanager
def mock_airflow_db() -> ContextManager[AirflowDb]:
    with tempfile.TemporaryDirectory() as temp_dir:
        test_db_path = os.path.join(temp_dir, 'airflow.db')
        sql_alchemy_conn = f'sqlite:///{test_db_path}'
        with set_airflow_db(sql_alchemy_conn, Fernet.generate_key().decode()):
            initdb()
            yield AirflowDb(sql_alchemy_conn=sql_alchemy_conn)


@contextlib.contextmanager
def set_airflow_db(sql_alchemy_conn: Optional[str], fernet_key: Optional[str]) -> ContextManager[AirflowDb]:
    env = {}
    if sql_alchemy_conn is not None:
        env['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = sql_alchemy_conn
    if fernet_key is not None:
        env['AIRFLOW__CORE__FERNET_KEY'] = fernet_key
    with set_env(**env):
        settings.configure_vars()
        settings.configure_orm()
        if sql_alchemy_conn is not None:
            assert str(settings.engine.url) == sql_alchemy_conn, f'{settings.engine.url} != {sql_alchemy_conn}'
        yield AirflowDb(sql_alchemy_conn or settings.SQL_ALCHEMY_CONN)
    settings.configure_vars()
    settings.configure_orm()


def set_connection(conn: dict):
    merge_conn(models.Connection(**conn))
