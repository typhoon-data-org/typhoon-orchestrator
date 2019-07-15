"""
This module is responsible for handling the lambda request and executing the correct task.
"""
from importlib import util
from pathlib import Path

import dateutil.parser

from typhoon.settings import typhoon_directory


class BrokenImportError(Exception):
    pass


def handle(event, context):
    if context['type'] == 'task':
        return handle_task(context)
    elif context['type'] == 'dag':
        raise NotImplementedError()
    else:
        raise NotImplementedError()


def handle_task(context):
    dag_name = context['dag_name']
    dag_path = Path(typhoon_directory()) / f'{dag_name}.py'
    module = _load_module_from_path(str(dag_path), module_name=dag_name)
    task_function = getattr(module, context['task_name'])

    dag_context = context['execution_context']
    dag_context['execution_date'] = dateutil.parser.parse(dag_context['execution_date'])
    module.DAG_CONFIG = dag_context
    return task_function(*context['args'], **context['kwargs'])


def _load_module_from_path(module_path, module_name):
    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (NameError, SyntaxError):
        raise BrokenImportError(f'Error loading module {module_path}')
    return module

