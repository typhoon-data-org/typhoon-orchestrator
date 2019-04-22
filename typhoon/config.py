import os
from configparser import ConfigParser
from typing import Any

from typhoon.logger import logger_factory
from typhoon.settings import typhoon_directory


class Config:
    def __init__(self, path: str, env: str):
        self.path = path
        self.env = env
        self.config = ConfigParser()
        self.config.read(self.path)

    def reload(self):
        self.config = ConfigParser().read(self.path)

    def get(self, var: str, default: Any = None, mandatory: bool = False) -> Any:
        if mandatory and var not in self.config[self.env.upper()].keys():
            raise ValueError(f'No attribute {var} in {self.env} config for {self.path}')
        return self.config[self.env.upper()].get(var, default)


class TyphoonConfig:
    """Parameters related to Typhoon Orchestrator configuration used at run-time (eg: DAG executions)"""
    def __init__(self, env):
        self.env = env
        self.config = Config(os.path.join(typhoon_directory(), 'typhoonconfig.cfg'), env)

    @property
    def logger(self):
        """Mandatory parameter defining what kind of Logger we will be using"""
        logger_name = self.config.get('logger', mandatory=True)
        return logger_factory(logger_name)

    @property
    def local_db(self):
        """If you are running a local instance of DynamoDB specify the endpoint"""
        return self.config.get('local-db')


class DeployConfig:
    """Parameters related to deploying an instance of Typhoon Orchestrator"""
    def __init__(self, deploy_env):
        self.env = deploy_env
        self.config = Config(os.path.join(typhoon_directory(), 'deployconfig.cfg'), deploy_env)

    @property
    def aws_profile(self):
        """Profile used to deploy Typhoon to the specified environment"""
        return self.config.get('aws-profile', mandatory=True)

    @property
    def project_name(self):
        """Name of the project used to give a unique name to resources in AWS"""
        return self.config.get('project-name', default='project')
