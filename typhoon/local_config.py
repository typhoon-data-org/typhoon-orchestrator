"""Reads config from typhoonconfig.cfg and loads it into environment variables"""
from pathlib import Path
from typing import Optional

EXAMPLE_CONFIG = """\
TYPHOON_PROJECT_NAME={project_name}
TYPHOON_DEPLOY_TARGET={deploy_target}
"""


def find_typhoon_cfg_in_cwd_or_parents() -> Optional[Path]:
    current_path = Path.cwd()
    while current_path != Path('/'):
        if 'typhoon.cfg' in [x.name for x in current_path.iterdir()]:
            return current_path / 'typhoon.cfg'
        current_path = current_path.parent
    return None
