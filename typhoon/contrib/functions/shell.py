import subprocess
from typing import Optional


def run(command: str, cwd: Optional[str] = None, env: Optional[dict] = None) -> bytes:
    """
    Run the specified command in a subprocess shell
    :param command: String representing the command to run
    :param cwd: Working directory (optional)
    :param env: Optional dictionary of environment variables
    :return:
    """
    p_target = subprocess.run(args=command, stdout=subprocess.PIPE, cwd=cwd, shell=True, env=env)
    yield p_target.stdout
