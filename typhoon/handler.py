"""
This module is responsible for handling the lambda request and executing the correct task.
"""
import json
from importlib import util
from pathlib import Path

import jsonpickle

from typhoon.models.dag_context import DagContext
from typhoon.settings import typhoon_directory


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
