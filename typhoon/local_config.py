"""Reads config from typhoonconfig.cfg and loads it into environment variables"""
from configparser import ConfigParser
from pathlib import Path
from typing import Optional

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


def find_typhoon_home_in_cwd_or_parents() -> Optional[Path]:
    current_path = Path.cwd()
    while current_path != Path('/'):
        if 'typhoon.cfg' in [x.name for x in current_path.iterdir()]:
            return current_path
        current_path = current_path.parent
    return None