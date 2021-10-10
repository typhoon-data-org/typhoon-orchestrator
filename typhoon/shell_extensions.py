import os
from types import SimpleNamespace

from IPython.core.magic import register_line_magic

from typhoon.core.dags import BrokenImportError
from typhoon.core.glue import load_dag_definition
from typhoon.core.runtime import DebugBroker
from typhoon.core.settings import Settings
from typhoon.core.transpiler.transpiler_helpers import camel_case
from typhoon.deployment.packaging import build_all_dags


def load_ipython_extension(ipython):
    @register_line_magic("load_dag")
    def load_dag(dag_name):
        tasks_path = str((Settings.out_directory/dag_name/'tasks.py').resolve())
        tasks_module = load_module_from_path(tasks_path, module_name='tasks')
        build_all_dags(None, matching=dag_name)
        dag = load_dag_definition(dag_name)
        ipython.user_ns['dag'] = dag
        task_instances = SimpleNamespace()
        for task_name, task in dag.tasks.items():
            if task.function is None:
                continue
            task_class = tasks_module.__dict__[f'{camel_case(task_name)}Task']
            task_instance = task_class(broker=DebugBroker())
            task_instance.set_destination('dummy')
            task_instances.__setattr__(task_name, task_instance)
        ipython.user_ns['tasks'] = task_instances

    @register_line_magic("reload_dag")
    def reload_dag(_):
        dag = ipython.user_ns.get('dag')
        if not dag:
            print(f'Cannot reload because no dags are currently loaded.')
            return
        load_dag(dag.name)


def load_module_from_path(module_path, module_name=None, must_exist=True):
    import sys
    from importlib import util

    # sys.path.append(os.path.dirname(os.path.dirname(module_path)))
    print(module_path)
    if module_name is None:
        parts = module_path.split('/')
        module_name = parts[-2] + '.' + parts[-1].strip('.py')
    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (NameError, SyntaxError, FileNotFoundError):
        if must_exist:
            raise BrokenImportError
        else:
            print(f'Module {module_name} at path {module_path} does not exist')
            return None
    return module
