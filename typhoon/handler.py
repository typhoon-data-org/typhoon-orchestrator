"""
This module is responsible for handling the lambda request and executing the correct task.
"""
import json
import os
import sys
from contextlib import contextmanager
from importlib import util
from io import StringIO
from pathlib import Path
from typing import Optional, List

import jsonpickle

from typhoon.models.dag_context import DagContext
from typhoon.settings import typhoon_directory, out_directory


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
    dag_name = event['dag_name']
    dag_path = Path(typhoon_directory()) / f'{dag_name}.py'
    module = _load_module_from_path(str(dag_path), module_name=dag_name)
    task_function = getattr(module, event['task_name'])

    kwargs = event['kwargs'].copy()
    kwargs['dag_context'] = DagContext.from_dict(kwargs['dag_context'])

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
    dag_path = Path(out_directory()) / dag_name / f'{dag_name}.py'

    if capture_logs:
        # Capture stdout
        stdout = sys.stdout
        sys.stdout = StringIO()
        with _sandbox_env():
            sys.path.append(str(dag_path.parent))
            module = _load_module_from_path(str(dag_path), module_name=f'{dag_name}.{dag_name}')
            main_function = getattr(module, f'{dag_name}_main')
            main_function({'time': time}, None)
        log = sys.stdout.getvalue()
        sys.stdout.close()
        sys.stdout = stdout
        return log
    else:
        with _sandbox_env():
            sys.path.append(str(dag_path.parent))
            module = _load_module_from_path(str(dag_path), module_name=f'{dag_name}.{dag_name}')
            main_function = getattr(module, f'{dag_name}_main')
            main_function({'time': time}, None)


if __name__ == '__main__':
    logs = run_dag('example_dag', '2019-01-03T01:00', capture_logs=True)
    a = 0
