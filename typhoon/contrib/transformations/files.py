from pathlib import Path
from typing import Union


def name(path: Union[str, Path]) -> str:
    return Path(path).name
