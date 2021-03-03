import json
import subprocess
from typing import Optional, List

from typing_extensions import TypedDict, Literal

from typhoon.contrib.hooks.singer_hook import SingerHook, build_singer_command


def run(
        hook: SingerHook,
        options: Optional[list] = None,
        message: Optional[str] = None,
        deserialize=False,
):
    with hook as tap:
        for msg_batch in tap.run(options, input_messages=[message] if message else None):
            if deserialize:
                msg_batch = [json.loads(msg) for msg in msg_batch.splitlines()]
            yield msg_batch


def run_pipeline(
        tap_hook: SingerHook,
        target_hook: SingerHook,
        tap_options: Optional[list] = None,
        target_options: Optional[list] = None,
):
    with tap_hook as tap, target_hook as target:
        tap_args = build_singer_command(tap.name, tap.config, tap_options, tap.venv)
        target_args = build_singer_command(target.name, target.config, target_options, target.venv)

        p_tap = subprocess.Popen(args=tap_args, stdout=subprocess.PIPE)
        p_target = subprocess.run(args=target_args, stdin=p_tap.stdout, stdout=subprocess.PIPE)

        yield p_target.stdout.decode()


class RecordMessage(TypedDict):
    type: Literal['RECORD']
    record: dict
    stream: str     # Name of stream
    time_extracted: Optional[str]   # RFC3339 format


class SchemaMessage(TypedDict):
    type: Literal['SCHEMA']
    schema: str     # JSON schema
    stream: str     # Name of stream
    key_properties: List[str]
    bookmark_properties: Optional[List[str]]