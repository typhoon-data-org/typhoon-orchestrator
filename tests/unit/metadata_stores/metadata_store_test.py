from pathlib import Path

import pytest

from typhoon.config import TyphoonConfig
from typhoon.connections import Connection
from typhoon.core.metadata_store_interface import MetadataStoreNotInitializedError
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
from typhoon.variables import Variable, VariableType

config_contents = """
[DEV]
project-name=typhoon_test
"""

sample_conn = Connection(conn_id='foo', conn_type='some_type')
sample_var = Variable(id='bar', type=VariableType.STRING, contents='hello world')


@pytest.fixture
def cfg_path(tmp_path, monkeypatch):
    cfg_path: Path = tmp_path / 'test.cfg'
    cfg_path.write_text(config_contents)
    monkeypatch.setenv('TYPHOON_HOME', str(tmp_path))
    return str(cfg_path)


def test_raise_metadata_store_not_initialized_error(cfg_path):
    config = TyphoonConfig('dev', config_path=cfg_path)
    store = SQLiteMetadataStore(config=config)
    with pytest.raises(MetadataStoreNotInitializedError):
        store.set_connection(sample_conn)


def test_set_and_get_connection(cfg_path):
    config = TyphoonConfig('dev', config_path=cfg_path)
    store = SQLiteMetadataStore(config=config)
    with store:
        store.set_connection(sample_conn)
        assert store.get_connection(sample_conn.conn_id) == sample_conn


def test_set_and_get_variable(cfg_path):
    config = TyphoonConfig('dev', config_path=cfg_path)
    store = SQLiteMetadataStore(config=config)
    with store:
        store.set_variable(sample_var)
        assert store.get_variable(sample_var.id) == sample_var

