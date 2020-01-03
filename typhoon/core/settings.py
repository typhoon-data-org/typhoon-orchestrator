import os
import re
from enum import Enum
from pathlib import Path
from typing import Union, Optional

from typing import TYPE_CHECKING
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


class _Settings:
    @property
    def typhoon_home(self) -> Path:
        return Path(os.environ[EnvVarName.PROJECT_HOME]) if EnvVarName.PROJECT_HOME in os.environ.keys() else None

    @typhoon_home.setter
    def typhoon_home(self, value: Union[str, Path]):
        os.environ[EnvVarName.PROJECT_HOME] = str(value)

    @property
    def typhoon_version(self) -> Path:
        return Path(os.environ[EnvVarName.PROJECT_VERSION]) if EnvVarName.PROJECT_VERSION in os.environ.keys() else 'latest'

    @typhoon_version.setter
    def typhoon_version(self, value: Union[str, Path]):
        os.environ[EnvVarName.PROJECT_VERSION] = str(value)

    @property
    def project_name(self) -> str:
        return os.environ[EnvVarName.PROJECT_NAME]

    @project_name.setter
    def project_name(self, value: str):
        os.environ[EnvVarName.PROJECT_NAME] = value

    @property
    def metadata_db_url(self):
        default_url = f'sqlite:{str(self.typhoon_home/self.project_name)}.db'
        return os.environ.get(EnvVarName.METADATA_DB_URL, default_url)

    @metadata_db_url.setter
    def metadata_db_url(self, value: str):
        os.environ[EnvVarName.METADATA_DB_URL] = value

    def metadata_store(self, aws_profile: Optional[str] = None) -> 'MetadataStoreInterface':
        if self.metadata_db_url.startswith('sqlite'):
            from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
            db_path = self.metadata_db_url.split(':')[1]
            return SQLiteMetadataStore(db_path=db_path)
        elif self.metadata_db_url.startswith('dynamodb'):
            from typhoon.metadata_store_impl.dynamodb_metadata_store import DynamodbMetadataStore
            host, region = re.match(r'dynamodb:Host=([^;]+);Region=([\w-]+)', self.metadata_db_url).groups()
            return DynamodbMetadataStore(host=host, region=region, aws_profile=aws_profile)
        else:
            ValueError(f'Metadata store type [{self.metadata_db_url.split(":")[0]}] not recognised')

    @property
    def metadata_suffix(self):
        return os.environ.get(EnvVarName.METADATA_SUFFIX)

    @metadata_suffix.setter
    def metadata_suffix(self, value: str):
        os.environ[EnvVarName.METADATA_SUFFIX] = value

    @property
    def dags_directory(self) -> Path:
        return Path(self.typhoon_home)/'dags'

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


Settings = _Settings()
