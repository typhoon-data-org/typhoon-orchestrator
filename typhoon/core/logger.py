import logging.config
from pathlib import Path

import yaml

from typhoon.core import settings


def setup_logging():
    logging_config_path = (Path(settings.typhoon_home()) / 'logger_config.yml')
    if logging_config_path.exists():
        config = logging_config_path.read_text()
        logging.config.dictConfig(yaml.load(config))
