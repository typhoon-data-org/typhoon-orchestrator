import pytest

from typhoon.connections import Connection
from typhoon.core.metadata_store_interface import MetadataObjectNotFound
from typhoon.core.settings import Settings
from typhoon.variables import Variable, VariableType

sample_conn = Connection(conn_id='foo', conn_type='some_type')
other_sample_conn = Connection(conn_id='bar', conn_type='some_other_type')

sample_var = Variable(id='foo', type=VariableType.STRING, contents='hello world')
other_sample_var = Variable(id='bar', type=VariableType.STRING, contents='hello world')


@pytest.fixture
def cfg_path(tmp_path):
    Settings.typhoon_home = tmp_path
    Settings.project_name = 'unittests'
    db_path = tmp_path / 'test.db'
    Settings.metadata_db_url = f'sqlite:{db_path}'
    return str(cfg_path)


def test_set_and_get_connection(cfg_path):
    store = Settings.metadata_store()
    store.set_connection(sample_conn)
    assert store.get_connection(sample_conn.conn_id) == sample_conn


def test_set_and_get_connections(cfg_path):
    store = Settings.metadata_store()
    store.set_connection(sample_conn)
    store.set_connection(other_sample_conn)
    assert store.get_connections() == [sample_conn, other_sample_conn] or \
        store.get_connections() == [other_sample_conn, sample_conn]
    assert store.get_connections(to_dict=True) == [sample_conn.__dict__, other_sample_conn.__dict__] or \
        store.get_connections(to_dict=True) == [other_sample_conn.__dict__, sample_conn.__dict__]


def test_delete_connection(cfg_path):
    store = Settings.metadata_store()
    store.set_connection(sample_conn)
    store.delete_connection(sample_conn)
    with pytest.raises(MetadataObjectNotFound):
        store.get_connection(sample_conn)
    with pytest.raises(MetadataObjectNotFound):
        store.get_connection(sample_conn.conn_id)


def test_set_and_get_variable(cfg_path):
    store = Settings.metadata_store()
    store.set_variable(sample_var)
    assert store.get_variable(sample_var.id) == sample_var


def test_set_and_get_variables(cfg_path):
    store = Settings.metadata_store()
    store.set_variable(sample_var)
    store.set_variable(other_sample_var)
    assert store.get_variables() == [sample_var, other_sample_var] or \
        store.get_variables() == [other_sample_var, sample_var]
    assert store.get_variables(to_dict=True) == [sample_var.__dict__, other_sample_var.__dict__] or \
        store.get_variables(to_dict=True) == [other_sample_var.__dict__, sample_var.__dict__]


def test_delete_variable(cfg_path):
    store = Settings.metadata_store()
    store.set_variable(sample_var)
    store.delete_variable(sample_var)
    with pytest.raises(MetadataObjectNotFound):
        store.get_variable(sample_var)
    with pytest.raises(MetadataObjectNotFound):
        store.get_variable(sample_var.id)

