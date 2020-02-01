import pytest

from typhoon.connections import Connection
from typhoon.core.settings import Settings
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
from typhoon.variables import Variable, VariableType


@pytest.fixture
def typhoon_home(tmp_path):
    Settings.typhoon_home = tmp_path
    Settings.project_name = 'unittests'
    db_path = tmp_path / 'test.db'
    Settings.metadata_db_url = f'sqlite:{db_path}'


def test_sqlite_metadata_store(typhoon_home):
    store = Settings.metadata_store()
    assert isinstance(store, SQLiteMetadataStore)

    conn = Connection(conn_id='foo', conn_type='s3')
    store.set_connection(conn)
    assert store.get_connection('foo') == conn

    var = Variable(id='bar', type=VariableType.STRING, contents='lorem ipsum')
    store.set_variable(var)
    assert store.get_variable('bar') == var
