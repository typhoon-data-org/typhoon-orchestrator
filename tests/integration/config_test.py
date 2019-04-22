import os
from tempfile import TemporaryDirectory

import pytest

from typhoon.config import TyphoonConfig, DeployConfig
from typhoon.logger import FileLogger

SAMPLE_TYPHOON_CONFIG = """\
[DEV]
logger=file
local-db=http://localhost:8000
"""


SAMPLE_DEPLOY_CONFIG = """\
[TEST]
aws-profile=testaws

[PROD]
aws-profile=prodaws
project-name=first_project
"""


@pytest.fixture
def typhoon_home(monkeypatch):
    with TemporaryDirectory() as typhoon_home_path:
        monkeypatch.setenv('TYPHOON_HOME', typhoon_home_path)
        with open(os.path.join(typhoon_home_path, 'typhoonconfig.cfg'), 'w') as f:
            f.write(SAMPLE_TYPHOON_CONFIG)
        with open(os.path.join(typhoon_home_path, 'deployconfig.cfg'), 'w') as f:
            f.write(SAMPLE_DEPLOY_CONFIG)
        yield typhoon_home_path


def test_typhoon_config(typhoon_home):
    assert os.environ['TYPHOON_HOME'] == typhoon_home

    typhoon_config = TyphoonConfig(env='dev')
    assert typhoon_config.logger == FileLogger
    assert typhoon_config.local_db == 'http://localhost:8000'

    deploy_config_test = DeployConfig(deploy_env='test')
    assert deploy_config_test.aws_profile == 'testaws'
    assert deploy_config_test.project_name == 'project'

    deploy_config_test = DeployConfig(deploy_env='prod')
    assert deploy_config_test.aws_profile == 'prodaws'
    assert deploy_config_test.project_name == 'first_project'
