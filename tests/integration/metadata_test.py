from contextlib import closing

import pytest

from typhoon.connections import Connection
from typhoon.core.config import TyphoonConfig
from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.variables import Variable, VariableType

SAMPLE_TYPHOON_CONFIG = """
[DEV]
project-name=typhoon_it
metadata-store-type=sqlite
"""


@pytest.fixture
def typhoon_home(tmp_path, monkeypatch):
    monkeypatch.setenv('TYPHOON_HOME', str(tmp_path))
    monkeypatch.setenv('TYPHOON_ENV', 'dev')
    (tmp_path / 'typhoonconfig.cfg').write_text(SAMPLE_TYPHOON_CONFIG)


def test_sqlite_metadata_store(typhoon_home):
    store_type = TyphoonConfig().metadata_store_type
    assert store_type == MetadataStoreType.sqlite
    with closing(TyphoonConfig().metadata_store) as store:
        conn = Connection(conn_id='foo', conn_type='s3')
        store.set_connection(conn)
        assert store.get_connection('foo') == conn

        var = Variable(id='bar', type=VariableType.STRING, contents='lorem ipsum')
        store.set_variable(var)
        assert store.get_variable('bar') == var
