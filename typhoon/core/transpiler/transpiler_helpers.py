import re
from typing import Iterable, Union, Any, List

from dataclasses import dataclass

from typhoon.core.components import Component
from typhoon.core.dags import DAGDefinitionV2, MultiStep, Py
from typhoon.core.templated import Templated


def get_transformations_modules(dag_or_component: Union[DAGDefinitionV2, Component]) -> Iterable[str]:
    def get_transformations_item(item) -> List[str]:
        if isinstance(item, Py) and 'transformations.' in item.value:
            return re.findall(r'(transformations\.\w+)\.\w+', item.value)
        elif isinstance(item, MultiStep):
            modules = []
            for x in item.value:
                mods = get_transformations_item(x)
                modules += mods
            return modules
        elif isinstance(item, list):
            modules = []
            for x in item:
                modules += get_transformations_item(x)
            return modules
        elif isinstance(item, dict):
            modules = []
            for k, v in item.items():
                modules += get_transformations_item(v)
            return modules
        else:
            return []

    modules = set()
    for task in dag_or_component.tasks.values():
        adapter, config = task.make_adapter_and_config()
        for val in adapter.values():
            mods = get_transformations_item(val)
            modules = modules.union(mods)
    return list(modules)


def get_typhoon_transformations_modules(dag_or_component: Union[DAGDefinitionV2, Component]) -> Iterable[str]:
    def get_typhoon_transformations_item(item) -> List[str]:
        if isinstance(item, Py) and 'typhoon.' in item.value:
            return re.findall(r'typhoon\.(\w+\.\w+)', item.value)
        elif isinstance(item, MultiStep):
            modules = []
            for x in item.value:
                mods = get_typhoon_transformations_item(x)
                modules += mods
            return modules
        elif isinstance(item, list):
            modules = []
            for x in item:
                modules += get_typhoon_transformations_item(x)
            return modules
        elif isinstance(item, dict):
            modules = []
            for k, v in item.items():
                modules += get_typhoon_transformations_item(v)
            return modules
        else:
            return []

    items = set()
    for task in dag_or_component.tasks.values():
        adapter, config = task.make_adapter_and_config()
        for val in adapter.values():
            items = items.union(x.split('.')[0] for x in get_typhoon_transformations_item(val))
    return list(items)


def get_functions_modules(dag_or_component: Union[DAGDefinitionV2, Component]) -> Iterable[str]:
    modules = set()
    for task in dag_or_component.tasks.values():
        if not task.function.startswith('typhoon.'):
            modules.add('.'.join(task.function.split('.')[:-1]))

    return list(modules)


def get_typhoon_functions_modules(dag_or_component: Union[DAGDefinitionV2, Component]) -> Iterable[str]:
    modules = set()
    for task in dag_or_component.tasks.values():
        if task.function.startswith('typhoon.'):
            modules.add(task.function.split('.')[1])

    return list(modules)


def expand_function(function: str) -> str:
    if not function.startswith('typhoon.'):
        return function
    else:
        _, module_name, function= function.split('.')
        return f'typhoon_function_{module_name}.{function}'


def camel_case(s: str) -> str:
    return s.title().replace('-', '_')


@dataclass
class TaskArgs(Templated):
    template = '''
    {% for key, value in args.items %}
    {% if value | is_literal %}
    args['{{ key }}'] = {{ value | clean_simple_param }}
    {% elif is_py %}
    args['{{ key }}'] = {{ value }}
    {% else %}
    {{ value }}
    {% endif %}
    {% endfor %}
    '''
    args: dict

    @staticmethod
    def is_literal(value):
        return not isinstance(value, (Py, MultiStep))

    @staticmethod
    def is_py(value):
        return isinstance(value, Py)

    @staticmethod
    def clean_simple_param(x):
        if not isinstance(x, str):
            return x
        return f'"""{x}"""' if "'" in x else f"'{x}'"
