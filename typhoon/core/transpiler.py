import os
import re
from collections import Sequence
from pathlib import Path
from typing import Union, List

import jinja2
import yaml

from typhoon import settings
from typhoon.core.dags import DAG


def get_dag_filenames():
    dags_directory = settings.dags_directory()
    # dag_files = Path(dags_directory).rglob('*.yml')
    dag_files = filter(lambda x: x.endswith('.yml'), os.listdir(dags_directory))
    return dag_files


def load_dags() -> Sequence[DAG]:
    dag_files = get_dag_filenames()
    for dag_file in dag_files:
        with open(os.path.join(settings.dags_directory(), dag_file), 'r') as f:
            yield DAG.from_dict_definition(yaml.load(f, Loader=yaml.FullLoader))


#################
# Jinja filters #
#################
def get_transformations_modules(dag: DAG) -> Sequence[str]:
    modules = set()
    for edge in dag.edges.values():
        for val in edge.adapter.values():
            if isinstance(val, str) and val.startswith('transformations.') and not val.startswith('typhoon.'):
                modules.add('.'.join(val.split('.')[:-1]))
            elif isinstance(val, list):
                for x in val:
                    if isinstance(x, str) and x.startswith('transformations.') and not x.startswith('typhoon.'):
                        modules.add('.'.join(x.split('.')[:-1]))

    return list(modules)


def get_functions_modules(nodes: dict) -> Sequence[str]:
    modules = set()
    for node in nodes.values():
        if not node['function'].startswith('typhoon.'):
            modules.add('.'.join(node['function'].split('.')[:-1]))

    return list(modules)


def clean_function_name(function_name: str, function_type: str) -> str:
    if not function_name.startswith('typhoon.'):
        return function_name
    else:
        parts = function_name.split('.')
        return '.'.join([f'typhoon_{function_type}', *parts[1:]])


def clean_simple_param(param: Union[str, int, float, List, dict]):
    if not isinstance(param, str):
        return param
    return f'"""{param}"""' if "'" in param else f"'{param}'"


def clean_param(param: Union[str, int, float, List, dict]):
    if isinstance(param, (int, float)):
        return param
    elif isinstance(param, str):
        if param.startswith('$dag_context.'):
            return f'dag_context["""{param.split(".")[-1]}"""]'
        elif "'" in param:
            return f'"""{param}"""'
        else:
            return f"'{param}'"
    elif isinstance(param, list):
        str_representation = ', '.join(f'{clean_param(x)}' for x in param)
        return f'[{str_representation}]'
    elif isinstance(param, dict):
        str_representation = ", ".join((f"{clean_param(k)}: {clean_param(v)}" for k, v in param.items()))
        return '{' + str_representation + '}'
    else:
        ValueError(f'Parameter {param} is not a recognised type: {type(param)}')


def substitute_special(code: str, key: str) -> str:
    if '=>' in key:
        key = key.replace(' ', '').split('=>')[0]
    code = code.replace('$BATCH', 'data')
    code = re.sub(r'\$dag_context(\.(\w+))', r'dag_context["""\g<2>"""]', code)
    code = code.replace('$dag_context', 'dag_context')
    code = re.sub(r'\$(\d)+', r"{key}_\g<1>".format(key=key), code)
    code = code.replace('$BATCH_NUM', 'batch_num')
    code = re.sub(r'\$HOOK(\.(\w+))', r'get_hook("\g<2>")', code)
    code = re.sub(r'\$VARIABLE(\.(\w+))', r'get_variable_contents("\g<2>")', code)
    return code


SEARCH_PATH = Path(__file__).parent / 'templates'
templateLoader = jinja2.FileSystemLoader(searchpath=str(SEARCH_PATH))
templateEnv = jinja2.Environment(loader=templateLoader)

templateEnv.trim_blocks = True
templateEnv.lstrip_blocks = True
templateEnv.keep_trailing_newline = True

templateEnv.globals.update(get_functions_modules=get_functions_modules)
templateEnv.globals.update(get_transformations_modules=get_transformations_modules)
templateEnv.filters.update(clean_function_name=clean_function_name)
templateEnv.filters.update(clean_param=clean_param)
templateEnv.filters.update(clean_simple_param=clean_simple_param)
templateEnv.filters.update(substitute_special=substitute_special)


def transpile(dag: DAG, env: str, debug_mode: bool = False):
    dag_template = templateEnv.get_template('dag_code.py.j2')
    return dag_template.render({'dag': dag, 'environment': env, 'debug_mode': debug_mode})
