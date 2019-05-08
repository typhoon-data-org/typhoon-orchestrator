# Define sentinels
from typing import Optional

from typhoon.config import CLIConfig, TyphoonConfig
from typhoon.settings import get_env

SKIP_BATCH = object()


def get_typhoon_config(use_cli_config: Optional[bool] = False, target_env: Optional[str] = None):
    """Reads the Typhoon Config file.
    If no target_env is specified it uses the TYPHOON-ENV environment variable.
    """
    env = target_env or get_env()
    return CLIConfig(env) if use_cli_config else TyphoonConfig(env)
