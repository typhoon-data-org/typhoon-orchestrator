import pytest

from typhoon.connections import Connection
from typhoon.core.settings import Settings
from typhoon.variables import Variable, VariableType

sample_conn = Connection(conn_id='foo', conn_type='some_type')
sample_var = Variable(id='bar', type=VariableType.STRING, contents='hello world')


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


def test_set_and_get_variable(cfg_path):
    store = Settings.metadata_store()
    store.set_variable(sample_var)
    assert store.get_variable(sample_var.id) == sample_var

