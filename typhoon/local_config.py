"""Reads config from typhoonconfig.cfg and loads it into environment variables"""
from configparser import ConfigParser

from typhoon.core.settings import Settings


EXAMPLE_CONFIG = """\
[CORE]
project-name={project_name}
"""


def _read_config():
    config = ConfigParser()
    config.read(str(Settings.typhoon_home/'typhoon.cfg'))
    return config


def project_name():
    return _read_config()['CORE']['project-name']
