import logging.config
from pathlib import Path

import yaml

from typhoon.core import settings


def setup_logging():
    config = (Path(settings.typhoon_directory()) / 'logger_config.yml').read_text()
    logging.config.dictConfig(yaml.load(config))
