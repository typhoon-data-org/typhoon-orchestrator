import os
import re
from enum import Enum
from pathlib import Path
from typing import Union, Optional

from typing import TYPE_CHECKING

from pydantic import BaseSettings, Field
from typhoon import local_config

if TYPE_CHECKING:
    from typhoon.core.metadata_store_interface import MetadataStoreInterface


def _join_underscores(*args) -> str:
    args = [x for x in args if x is not None]
    return '_'.join(args)


class EnvVarName(str, Enum):
    PROJECT_HOME = 'TYPHOON_HOME'
    PROJECT_NAME = 'TYPHOON_PROJECT_NAME'
    PROJECT_VERSION = 'TYPHOON_VERSION'
    METADATA_DB_URL = 'TYPHOON_METADATA_DB_URL'
    METADATA_SUFFIX = 'TYPHOON_METADATA_SUFFIX'
    DEPLOY_TARGET = 'TYPHOON_DEPLOY_TARGET'
    FERNET_KEY = 'TYPHOON_FERNET_KEY'


class TyphoonSettingsFile(BaseSettings):
    typhoon_home: Path = Field(default=None, env=EnvVarName.PROJECT_HOME)
    typhoon_version: str = Field(default='latest', env=EnvVarName.PROJECT_VERSION)
    project_name: str = Field(default='project', env=EnvVarName.PROJECT_NAME)
    metadata_db_url_: str = Field(default=None, env=EnvVarName.METADATA_DB_URL)
    metadata_suffix: str = Field(default='', env=EnvVarName.METADATA_SUFFIX)
    deploy_target: str = Field(default='typhoon', env=EnvVarName.DEPLOY_TARGET)
    fernet_key: str = Field(default=None, env=EnvVarName.FERNET_KEY)

    # class Config:
    #     env_file = 'typhoon.cfg'


class TyphoonSettings:
    typhoon_home: Path
    typhoon_version: str
    project_name: str
    metadata_db_url_: str
    metadata_suffix: str
    deploy_target: str
    fernet_key: str

    def __init__(self, _env_file: str = None):
        if _env_file is None and os.environ.get(EnvVarName.PROJECT_HOME):
            _env_file = Path(os.environ.get(EnvVarName.PROJECT_HOME)) / 'typhoon.cfg'
        self._settings = TyphoonSettingsFile(_env_file=_env_file).dict()

    def set(self, _env_file: str = None):
        self._settings = TyphoonSettingsFile(_env_file=_env_file).dict()

    def __getattr__(self, item):
        return self._settings[item]

    def export_vars(self):
        if self.typhoon_home:
            os.environ[EnvVarName.PROJECT_HOME] = str(self.typhoon_home)
        if self.typhoon_version:
            os.environ[EnvVarName.PROJECT_VERSION] = self.typhoon_version
        if self.project_name:
            os.environ[EnvVarName.PROJECT_NAME] = self.project_name
        if self.metadata_db_url_:
            os.environ[EnvVarName.METADATA_DB_URL] = self.metadata_db_url_
        if self.metadata_suffix:
            os.environ[EnvVarName.METADATA_SUFFIX] = self.metadata_suffix
        if self.deploy_target:
            os.environ[EnvVarName.DEPLOY_TARGET] = self.deploy_target
        if self.fernet_key:
            os.environ[EnvVarName.FERNET_KEY] = self.fernet_key

    @property
    def default_sqlite_path(self) -> str:
        return f'sqlite:{str(self.typhoon_home / self.project_name)}.db'

    @property
    def metadata_db_url(self):
        if self.metadata_db_url_:
            return self.metadata_db_url_
        elif self.deploy_target == 'airflow':
            return None
        else:
            return self.default_sqlite_path

    @metadata_db_url.setter
    def metadata_db_url(self, value: str):
        self.metadata_db_url_ = value
        os.environ[EnvVarName.METADATA_DB_URL] = value

    @property
    def dags_directory(self) -> Path:
        return Path(self.typhoon_home)/'dags'

    @property
    def components_directory(self) -> Path:
        return Path(self.typhoon_home)/'components'

    @property
    def out_directory(self) -> Path:
        return Path(self.typhoon_home)/'out'

    @property
    def functions_directory(self) -> Path:
        return Path(self.typhoon_home) / 'functions'

    @property
    def transformations_directory(self) -> Path:
        return Path(self.typhoon_home) / 'transformations'

    @property
    def hooks_directory(self) -> Path:
        return Path(self.typhoon_home) / 'hooks'

    @property
    def connections_table_name(self) -> str:
        return _join_underscores('typhoon', self.project_name, 'connections', self.metadata_suffix)

    @property
    def variables_table_name(self) -> str:
        return _join_underscores('typhoon', self.project_name, 'variables', self.metadata_suffix)

    @property
    def dag_deployments_table_name(self) -> str:
        return _join_underscores('typhoon', self.project_name, 'deployments', self.metadata_suffix)

    def metadata_store(self, aws_profile: Optional[str] = None) -> 'MetadataStoreInterface':
        if self.deploy_target == 'airflow':
            from typhoon.metadata_store_impl.airflow_metadata_store import AirflowMetadataStore
            return AirflowMetadataStore(self.metadata_db_url)   # Todo: Use remote path
        elif self.metadata_db_url.startswith('sqlite'):
            from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
            db_path = self.metadata_db_url.split(':')[1]
            return SQLiteMetadataStore(db_path=db_path)
        elif self.metadata_db_url.startswith('dynamodb'):
            from typhoon.metadata_store_impl.dynamodb_metadata_store import DynamodbMetadataStore
            host, region = re.match(r'dynamodb:Host=([^;]+);Region=([\w-]+)', self.metadata_db_url).groups()
            return DynamodbMetadataStore(host=host, region=region, aws_profile=aws_profile)
        else:
            ValueError(f'Metadata store type [{self.metadata_db_url.split(":")[0]}] not recognised')


def set_settings_from_file(settings_file: Path):
    Settings.set(_env_file=str(settings_file))
    Settings.export_vars()


Settings = TyphoonSettings()

if Settings.typhoon_home:
    print(f'${EnvVarName.PROJECT_HOME} defined from env variable to "{Settings.typhoon_home}"')
else:
    typhoon_config_file = local_config.find_typhoon_cfg_in_cwd_or_parents()
    if os.environ.get('_TYPHOON_COMPLETE') in ['source', 'source_bash', 'source_zsh', 'source_fish']:
        # We just want click to generate a shell script that can be evaluated to provide code complete
        # Anything we print will be evaluated as a shell script and cause errors so we just do nothing
        pass
    elif not typhoon_config_file:
        print('Did not find typhoon.cfg in current directory or any of its parent directories')
    else:
        os.environ[EnvVarName.PROJECT_HOME] = str(typhoon_config_file.parent)
        set_settings_from_file(typhoon_config_file)
        if not Settings.project_name:
            print(f'Project name not set in "{Settings.typhoon_home}/typhoon.cfg "')
