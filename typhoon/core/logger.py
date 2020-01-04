import logging.config
import sys

import yaml

from typhoon.core.settings import Settings


def setup_logging():
    logging_config_path = Settings.typhoon_home / 'logger_config.yml'
    if logging_config_path.exists():
        config = logging_config_path.read_text()
        logging.config.dictConfig(yaml.load(config))
    else:
        logging.basicConfig(
            stream=sys.stdout,
            format='[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s',
            level=logging.INFO,
        )
