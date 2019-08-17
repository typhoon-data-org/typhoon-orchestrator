import os
import re
from configparser import ConfigParser
from typing import Any, Type, Union, Optional

from typhoon.aws.plumbing.dynamodb_plumbing import dynamodb_connection, DynamoDBConnectionType
from typhoon.logger_interface import LoggingInterface
from typhoon.core.settings import typhoon_directory


class TyphoonConfigError(Exception):
    pass


class Config:
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
    def __init__(self, env, config_path=None):
        self.env = env
        self.config_path = config_path or 'typhoonconfig.cfg'
        self.config = Config(os.path.join(typhoon_directory(), self.config_path), env)

    @property
    def logger(self) -> Type[LoggingInterface]:
        """Mandatory parameter defining what kind of Logger we will be using"""
        # TODO: Add logging to elasticsearch
        logger_type = self.config.get('logger')
        if logger_type not in ['s3', 'file', 'None', None]:
            raise ValueError(f'Logger {logger_type} is not a valid option')
        return logger_type

    @property
    def local_log_path(self) -> Optional[str]:
        """If logger is set to file set the path where we should log"""
        return self.config.get('local-log-path')

    @property
    def local_db(self) -> str:
        """If you are running a local instance of DynamoDB specify the endpoint"""
        return self.config.get('local-db')

    @property
    def project_name(self) -> str:
        """Name of the project used to give a unique name to resources in AWS"""
        return self.config.get('project-name', default='project')

    @property
    def s3_bucket(self) -> str:
        """Name of the S3 bucket where we will do our deployment (calculated from project-name)"""
        return f'typhoon-{self.project_name.lower()}'

    @property
    def connections_table_name(self) -> str:
        """Name of the DynamoDB table where we will store connections (calculated from project-name)"""
        return f'typhoon_{self.project_name.lower()}_connections'

    @property
    def connections_table_read_capacity_units(self) -> int:
        return self.config.get('connections-table-read-capacity-units', default=1)

    @property
    def connections_table_write_capacity_units(self) -> int:
        return self.config.get('connections-table-write-capacity-units', default=1)

    @property
    def variables_table_name(self) -> str:
        """Name of the DynamoDB table where we will store variables (calculated from project-name)"""
        return f'typhoon_{self.project_name.lower()}_variables'

    @property
    def variables_table_read_capacity_units(self) -> int:
        return self.config.get('variables-table-read-capacity-units', default=1)

    @property
    def variables_table_write_capacity_units(self) -> int:
        return self.config.get('variables-table-write-capacity-units', default=1)

    @property
    def dynamodb_region(self) -> str:
        return self.config.get('dynamodb-region')

    @property
    def deploy_region(self) -> str:
        return self.config.get('deploy-region', mandatory=True)

    @property
    def dynamodb_endpoint(self) -> str:
        return self.config.get('dynamodb-endpoint')

    @property
    def dynamodb_resource(self):
        return dynamodb_connection(
            conn_type=DynamoDBConnectionType.RESOURCE,
            aws_region=self.dynamodb_region,
            endpoint_url=self.dynamodb_endpoint,
        )

    @property
    def dynamodb_client(self):
        return dynamodb_connection(
            conn_type=DynamoDBConnectionType.CLIENT,
            aws_region=self.dynamodb_region,
            endpoint_url=self.dynamodb_endpoint,
        )

    @property
    def iam_role_name(self):
        return f'typhoon_{self.project_name}_role'

    @property
    def lambda_function_timeout(self) -> str:
        return self.config.get('lambda-function-timeout', 50)


class CLIConfig(TyphoonConfig):
    """
    Parameters related to deploying an instance of Typhoon Orchestrator and other CLI operations.
    Has access to all the attributes defined in TyphoonConfig as well as others only specific to CLIConfig.
    """
    def __init__(self, target_env):
        config_path = 'cliconfig.cfg'
        super().__init__(target_env, config_path=config_path)
        self.env = target_env
        self.config = Config(os.path.join(typhoon_directory(), config_path), target_env)

    @property
    def aws_profile(self) -> Optional[str]:
        """Profile used to deploy Typhoon to the specified environment"""
        return self.config.get('aws-profile')

    @property
    def dynamodb_resource(self):
        aws_profile = self.aws_profile
        return dynamodb_connection(
            aws_profile=aws_profile,
            conn_type=DynamoDBConnectionType.RESOURCE,
            aws_region=self.dynamodb_region,
            endpoint_url=self.dynamodb_endpoint,
        )

    @property
    def dynamodb_client(self):
        aws_profile = self.aws_profile
        return dynamodb_connection(
            aws_profile=aws_profile,
            conn_type=DynamoDBConnectionType.CLIENT,
            aws_region=self.dynamodb_region,
            endpoint_url=self.dynamodb_endpoint,
        )

    @property
    def typhoon_version(self) -> Optional[str]:
        return self.config.get('typhoon-version', default='latest')

    def typhoon_version_is_local(self):
        version = self.typhoon_version
        return version and version != 'latest' and not re.match(r'\d\.\d\.\d', version)


TyphoonConfigType = Union[TyphoonConfig, CLIConfig]
