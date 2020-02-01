from typhoon.core.settings import Settings
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore


def test_typhoon_config(tmp_path):
    Settings.typhoon_home = tmp_path
    Settings.metadata_db_url = f'sqlite:{tmp_path/"test.db"}'
    Settings.project_name = 'integrationtests'

    assert Settings.typhoon_home == tmp_path

    assert isinstance(Settings.metadata_store(), SQLiteMetadataStore)
    assert Settings.project_name == 'integrationtests'
