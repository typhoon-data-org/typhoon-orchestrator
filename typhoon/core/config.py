import os
import re
from configparser import ConfigParser
from typing import Any, Union, Optional

from typhoon.core import settings
from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.metadata_store_impl.metadata_store_factory import metadata_store_factory
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typhoon.core.metadata_store_interface import MetadataStoreInterface


class TyphoonConfigError(Exception):
    pass


class Config:
    """Base config class. Do not instantiate and use directly outside this module. Use TyphoonConfig"""
    def __init__(self, path: str, env: str):
        self.path = path
        self.env = env
        self.config = ConfigParser()
        self.config.read(self.path)

    def reload(self):
        self.config = ConfigParser().read(self.path)

    def get(self, var: str, default: Any = None, mandatory: bool = False) -> Any:
        env = self.env.upper()
        if env not in self.config.keys():
            raise TyphoonConfigError(f'Environment {env} not in {self.path}')

        if mandatory and var not in self.config[env].keys():
            raise TyphoonConfigError(f'No attribute {var} in {env} config for {self.path}')
        return self.config[env].get(var, default)


class TyphoonConfig:
    """Parameters related to Typhoon Orchestrator configuration used at run-time (eg: DAG executions)"""
    def __init__(self, env=None, config_path=None):
        self.env = env or settings.environment()
        self.config_path = config_path or 'typhoonconfig.cfg'
        self.config = Config(os.path.join(settings.typhoon_home(), self.config_path), self.env)

    @property
    def project_name(self) -> str:
        """Name of the project used to give a unique name to resources in AWS"""
        return self.config.get('project-name', default='project')

    @property
    def connections_table_name(self) -> str:
        """Name of the DynamoDB table where we will store connections (calculated from project-name)"""
        return f'typhoon_{self.project_name.lower()}_connections'

    @property
    def variables_table_name(self) -> str:
        """Name of the DynamoDB table where we will store variables (calculated from project-name)"""
        return f'typhoon_{self.project_name.lower()}_variables'

    @property
    def metadata_store_type(self) -> MetadataStoreType:
        store_type = self.config.get('metadata-store-type', default='sqlite')
        return MetadataStoreType.from_string(store_type)

    @property
    def metadata_store(self) -> 'MetadataStoreInterface':
        store_class = metadata_store_factory(self.metadata_store_type)
        return store_class(config=self)

    @property
    def dynamodb_region(self) -> str:
        return self.config.get('dynamodb-region')

    @property
    def dynamodb_endpoint(self) -> str:
        return self.config.get('dynamodb-endpoint')


class CLIConfig(TyphoonConfig):
    """
    Parameters related to deploying an instance of Typhoon Orchestrator and other CLI operations.
    Has access to all the attributes defined in TyphoonConfig as well as others only specific to CLIConfig.
    """
    def __init__(self, target_env):
        config_path = 'cliconfig.cfg'
        super().__init__(target_env, config_path=config_path)
        self.env = target_env
        self.config = Config(os.path.join(settings.typhoon_home(), config_path), target_env)

    @property
    def aws_profile(self) -> Optional[str]:
        """Profile used to deploy Typhoon to the specified environment"""
        return self.config.get('aws-profile')

    # @property
    # def dynamodb_resource(self):
    #     aws_profile = self.aws_profile
    #     return dynamodb_connection(
    #         aws_profile=aws_profile,
    #         conn_type=DynamoDBConnectionType.RESOURCE,
    #         aws_region=self.dynamodb_region,
    #         endpoint_url=self.dynamodb_endpoint,
    #     )

    # @property
    # def dynamodb_client(self):
    #     aws_profile = self.aws_profile
    #     return dynamodb_connection(
    #         aws_profile=aws_profile,
    #         conn_type=DynamoDBConnectionType.CLIENT,
    #         aws_region=self.dynamodb_region,
    #         endpoint_url=self.dynamodb_endpoint,
    #     )

    @property
    def typhoon_version(self) -> Optional[str]:
        return self.config.get('typhoon-version', default='latest')

    def typhoon_version_is_local(self):
        version = self.typhoon_version
        return version and version != 'latest' and not re.match(r'\d\.\d\.\d', version)


TyphoonConfigType = Union[TyphoonConfig, CLIConfig]
