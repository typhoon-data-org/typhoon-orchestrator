import subprocess
from typing import List, Optional

from dataclasses import dataclass

from typhoon.contrib.hooks.hook_interface import HookInterface


class SingerHook(HookInterface):
    conn_type = 'singer'

    def __init__(self, conn_params):
        self.conn_params = conn_params

    @property
    def tap(self) -> str:
        return self.conn_params.extra.get('tap')

    @property
    def config(self) -> dict:
        return self.conn_params.extra.get('config')

    @property
    def venv(self) -> str:
        return self.conn_params.extra.get('venv')

    def __enter__(self) -> 'SingerTapOrTarget':
        return SingerTapOrTarget(self.tap, self.config, self.venv)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn_params = None


def build_singer_command(tap_or_target: str, config: dict, options: list, venv) -> List[str]:
    args = [venv + tap_or_target]
    if config:
        args += ['--config', config]
    if options:
        args += options
    return args


@dataclass
class SingerTapOrTarget:
    name: str
    config: Optional[dict]
    venv: str = ''

    def run(self, options: list = None, input_messages: List[str] = None) -> str:
        args = build_singer_command(self.name, self.config, options, self.venv)
        p = subprocess.run(args=args, input='\n'.join(input_messages or []).encode(), stdout=subprocess.PIPE)
        yield p.stdout.decode()


