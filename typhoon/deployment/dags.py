import os
import re
from typing import Sequence, Tuple, Iterable, Union, List

import jinja2 as jinja2
import yaml

from typhoon import settings


def load_dags() -> Sequence:
    dags_directory = settings.dags_directory()
    dags = []

    dag_files = filter(lambda x: x.endswith('.yml'), os.listdir(dags_directory))
    for dag_file in dag_files:
        with open(os.path.join(dags_directory, dag_file), 'r') as f:
            dag = yaml.load(f)
            if dag.get('active', True):
                dag['structure'] = build_dag_structure(dag['edges'])
                dags.append(dag)

    return dags


def build_dag_structure(edges: dict) -> dict:
    structure = {}
    for _, edge in edges.items():
        if edge['source'] not in structure.keys():
            structure[edge['source']] = [edge['destination']]
        else:
            structure[edge['source']].append(edge['destination'])

    return structure


def get_sources(structure: dict) -> Sequence[str]:
    sources = set(structure.keys())
    destinations = set(get_destinations(structure))
    return list(sources.difference(destinations))


def get_sinks(structure: dict) -> Sequence[str]:
    sources = set(structure.keys())
    destinations = set(get_destinations(structure))
    return list(destinations.difference(sources))


def get_transformations(edges: dict, source: str, destination: str) -> Sequence[str]:
    for _, edge in edges.items():
        if edge['source'] == source and edge['destination'] == destination:
            return edge['transformations']


def get_edge(edges: dict, source: str, destination: str) -> Tuple[str, str]:
    for edge_name, edge in edges.items():
        if edge['source'] == source and edge['destination'] == destination:
            return edge_name, edge


def get_edges_for_source(edges, source) -> Tuple[str, str]:
    for edge_name, edge in edges.items():
        if edge['source'] == source:
            yield edge_name, edge


def get_destinations(structure) -> Iterable[str]:
    for x in structure.values():
        for edge in x:
            yield edge


def get_adapters_modules(edges: dict)  -> Iterable[str]:
    modules = set()
    for _, edge in edges.items():
        if not edge['adapter']['function'].startswith('typhoon.'):
            modules.add('.'.join(edge['adapter']['function'].split('.')[:-1]))

    return list(modules)


def get_transformations_modules(edges: dict) -> Iterable[str]:
    modules = set()
    for _, edge in edges.items():
        for _, val in edge['adapter'].items():
            if not isinstance(val, list):
                if isinstance(val, str) and val.startswith('transformations.') and not val.startswith('typhoon.'):
                    modules.add('.'.join(val.split('.')[:-1]))
            else:
                for x in val:
                    if isinstance(x, str) and x.startswith('transformations.') and not x.startswith('typhoon.'):
                        modules.add('.'.join(x.split('.')[:-1]))

    return list(modules)


def get_functions_modules(nodes: dict) -> Iterable[str]:
    modules = set()
    for _, node in nodes.items():
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
    if isinstance(param, str):
        if "'" in param:
            return f'"""{param}"""'
        else:
            return f"'{param}'"
    else:
        return param


def substitute_special(code: str, key: str) -> str:
    if '=>' in key:
        key = key.replace(' ', '').split('=>')[0]
    code = code.replace('$SOURCE', 'data')
    code = re.sub(r'\$DAG_CONFIG(\.(\w+))', r'DAG_CONFIG["""\g<2>"""]', code)
    code = code.replace('$DAG_CONFIG', 'DAG_CONFIG')
    code = re.sub(r'\$(\d)+', r"{key}_\g<1>".format(key=key), code)
    code = code.replace('$BATCH_NUM', 'batch_num')
    code = re.sub(r'\$HOOK(\.(\w+))', r'get_hook("\g<2>")', code)
    code = re.sub(r'\$VARIABLE(\.(\w+))', r'get_variable_contents("\g<2>")', code)
    return code


def clean_param(param: Union[str, int, float, List, dict]):
    if isinstance(param, (int, float)):
        return param
    elif isinstance(param, str):
        if param.startswith('$DAG_CONFIG.'):
            return f'DAG_CONFIG["""{param.split(".")[-1]}"""]'
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


SEARCH_PATH = os.path.join(
    os.path.dirname(__file__),
    'templates')
templateLoader = jinja2.FileSystemLoader(searchpath=SEARCH_PATH)
templateEnv = jinja2.Environment(loader=templateLoader)

templateEnv.trim_blocks = True
templateEnv.lstrip_blocks = True
templateEnv.keep_trailing_newline = True

templateEnv.globals.update(get_sources=get_sources)
templateEnv.globals.update(get_sinks=get_sinks)
templateEnv.globals.update(get_destinations=get_destinations)
templateEnv.globals.update(get_transformations=get_transformations)
templateEnv.globals.update(get_adapters_modules=get_adapters_modules)
templateEnv.globals.update(get_functions_modules=get_functions_modules)
templateEnv.globals.update(get_transformations_modules=get_transformations_modules)
templateEnv.globals.update(get_edge=get_edge)
templateEnv.globals.update(get_edges_for_source=get_edges_for_source)

templateEnv.filters.update(clean_function_name=clean_function_name)
templateEnv.filters.update(clean_param=clean_param)
templateEnv.filters.update(clean_simple_param=clean_simple_param)
templateEnv.filters.update(substitute_special=substitute_special)


def generate_dag_code(dag: dict, env: str):
    dag_template = templateEnv.get_template('dag_code.py.j2')
    dag['environment'] = env
    return dag_template.render(dag)

