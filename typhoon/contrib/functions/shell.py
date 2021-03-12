import subprocess
from typing import Optional


def run(command: str, cwd: Optional[str] = None, env: Optional[dict] = None) -> bytes:
    p_target = subprocess.run(args=command, stdout=subprocess.PIPE, cwd=cwd, shell=True, env=env)
    yield p_target.stdout
