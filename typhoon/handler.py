"""
This module is responsible for handling the lambda request and executing the correct task.
"""
import json
import os
import sys
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from importlib import util
from io import StringIO
from typing import Optional

import jsonpickle

from typhoon.core import setup_logging
from typhoon.core.dags import DagContext
from typhoon.core.settings import Settings


class BrokenImportError(Exception):
    pass


def handle(event, context):
    if event['type'] == 'task':
        return handle_task(jsonpickle.decode(json.dumps(event)))
    elif event['type'] == 'dag':
        raise NotImplementedError()
    else:
        raise NotImplementedError()


def handle_task(event):
    setup_logging()
    dag_name = event['dag_name']
    dag_path = Settings.typhoon_home / f'{dag_name}.py'
    module = _load_module_from_path(str(dag_path), module_name=dag_name)
    task_function = getattr(module, event['task_name'])

    kwargs = event['kwargs'].copy()
    kwargs['dag_context'] = DagContext.parse_obj(kwargs['dag_context'])

    return task_function.sync(*event['args'], **kwargs)


def _load_module_from_path(module_path, module_name):
    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (NameError, SyntaxError):
        raise BrokenImportError(f'Error loading module {module_path}')
    return module


@contextmanager
def _sandbox_env():
    env_bk = os.environ.copy()
    sys_path_bk = sys.path[:]
    yield
    sys.path = sys_path_bk
    os.environ = env_bk


def run_dag(dag_name, time, capture_logs: bool = False) -> Optional[str]:
    dag_path = Settings.out_directory / dag_name / f'{dag_name}.py'

    if capture_logs:
        # Capture stdout
        # stdout = sys.stdout
        stdout_buffer = StringIO()
        # sys.stdout = stdout_buffer
        stderr_buffer = StringIO()
        # stderr = stderr_buffer
        # sys.stderr =stderr_buffer
        with _sandbox_env(), redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            sys.path.append(str(dag_path.parent))
            module = _load_module_from_path(str(dag_path), module_name=f'{dag_name}.{dag_name}')
            main_function = getattr(module, f'{dag_name}_main')
            main_function({'time': time}, None)
        log = stdout_buffer.getvalue()
        err_log = stderr_buffer.getvalue()
        # sys.stdout = stdout
        # stdout_buffer.close()
        # sys.stderr = stderr
        # stderr_buffer.close()
        return log + '\n' + err_log
    else:
        with _sandbox_env():
            sys.path.append(str(dag_path.parent))
            module = _load_module_from_path(str(dag_path), module_name=f'{dag_name}.{dag_name}')
            main_function = getattr(module, f'{dag_name}_main')
            main_function({'time': time}, None)


if __name__ == '__main__':
    logs = run_dag('example_dag', '2019-01-03T01:00', capture_logs=True)
    a = 0
