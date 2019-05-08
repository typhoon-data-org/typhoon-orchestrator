import pytest
from moto import mock_dynamodb2

from tests.integration.fixtures import set_dev_env, mock_typhoon_home
from typhoon.aws.exceptions import TyphoonResourceNotFoundError
from typhoon.aws.porcelain.dynamodb_porcelain import dynamodb_table_exists
from typhoon.connections import set_connection, Connection, get_connection, get_connection_params, delete_connection
from typhoon.core import get_typhoon_config
from typhoon.deployment.dynamo import create_connections_table, create_variables_table
from typhoon.variables import Variable, VariableType, set_variable, get_variable, get_variable_contents, delete_variable


@mock_dynamodb2
def test_create_connections_table(set_dev_env, mock_typhoon_home):
    config = get_typhoon_config()

    assert not dynamodb_table_exists(config.connections_table_name)
    create_connections_table()
    assert dynamodb_table_exists(config.connections_table_name)

    test_conn = Connection(conn_id='foo', conn_type='s3')
    set_connection(test_conn.conn_id, test_conn.get_connection_params())

    conn = get_connection(test_conn.conn_id)
    assert test_conn.__dict__ == conn.__dict__

    conn_params = get_connection_params(test_conn.conn_id)
    assert conn_params.__dict__ == test_conn.get_connection_params().__dict__

    delete_connection(test_conn.conn_id)
    with pytest.raises(TyphoonResourceNotFoundError):
        get_connection(test_conn.conn_id)


@mock_dynamodb2
def test_create_variables_table(set_dev_env, mock_typhoon_home):
    config = get_typhoon_config()

    assert not dynamodb_table_exists(config.variables_table_name)
    create_variables_table()
    assert dynamodb_table_exists(config.variables_table_name)

    test_var = Variable(id='bar', type=VariableType.JSON, contents='[ {"aa": 1}, true]')
    set_variable(test_var)

    var = get_variable(test_var.id)
    assert test_var.dict_contents() == var.dict_contents()

    assert test_var.get_contents() == [{'aa': 1}, True]
    var_contents = get_variable_contents(test_var.id)
    assert var_contents == test_var.get_contents()

    delete_variable(test_var.id)
    with pytest.raises(TyphoonResourceNotFoundError):
        get_variable_contents(test_var.id)
