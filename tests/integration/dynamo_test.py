import pytest

from typhoon.aws.exceptions import TyphoonResourceNotFoundError
from typhoon.connections import Connection
from typhoon.core.settings import Settings
from typhoon.variables import Variable, VariableType


@pytest.fixture()
def ddb_store():
    Settings.typhoon_home = '/tmp/foo'
    Settings.project_name = 'unittests'
    Settings.metadata_db_url = f'dynamodb:Host=localhost:8080;Region=eu-west-1'

    ddb_store = Settings.metadata_store()
    ddb_store.migrate()
    return ddb_store


def test_create_connections_table(ddb_store):
    assert ddb_store.exists()

    test_conn = Connection(conn_id='foo', conn_type='s3')
    ddb_store.set_connection(test_conn)

    assert test_conn == ddb_store.get_connection(conn_id=test_conn.conn_id)

    ddb_store.delete_connection(conn=test_conn)
    with pytest.raises(TyphoonResourceNotFoundError):
        ddb_store.get_connection(test_conn.conn_id)


def test_create_variables_table(ddb_store):
    test_var = Variable(id='bar', type=VariableType.JSON, contents='[ {"aa": 1}, true]')
    ddb_store.set_variable(test_var)

    assert test_var == ddb_store.get_variable(test_var.id)

    assert test_var.get_contents() == [{'aa': 1}, True]

    ddb_store.delete_variable(test_var.id)
    with pytest.raises(TyphoonResourceNotFoundError):
        ddb_store.get_variable(test_var.id)
