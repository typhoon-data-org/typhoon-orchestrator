import os
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture
def set_dev_env(monkeypatch):
    monkeypatch.setenv('TYPHOON_ENV', 'dev')
    yield


@pytest.fixture
def mock_typhoon_home(monkeypatch):
    sample_typhoon_config = """\
[DEV]
project-name=integration_tests
"""
    with TemporaryDirectory() as typhoon_home_path:
        monkeypatch.setenv('TYPHOON_HOME', typhoon_home_path)
        with open(os.path.join(typhoon_home_path, 'typhoonconfig.cfg'), 'w') as f:
            f.write(sample_typhoon_config)
        yield typhoon_home_path
