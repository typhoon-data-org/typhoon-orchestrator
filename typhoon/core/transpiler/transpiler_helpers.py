import re
from typing import Iterable, Union, List, Tuple, Dict, NamedTuple, Optional, Set

from dataclasses import dataclass
from typing_extensions import Literal

from typhoon.core.components import Component
from typhoon.core.dags import DAGDefinitionV2, MultiStep, Py, TaskDefinition
from typhoon.core.templated import Templated
from typhoon.introspection.introspect_extensions import ExtensionsInfo, get_typhoon_extensions_info


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
        adapter, _ = task.make_adapter_and_config()
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
        _, module_name, function = function.split('.')
        return f'typhoon_functions_{module_name}.{function}'


def camel_case(s: str) -> str:
    return s.title().replace('-', '').replace('_', '')


@dataclass
class TaskArgs(Templated):
    template = '''
    {% for key, value in args.items() %}
    {% if value | is_literal %}
    args['{{ key }}'] = {{ value | clean_simple_param }}
    {% elif value | is_py %}
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


def render_dependencies(dependencies: List[Tuple[str, str]]) -> str:
    return DependenciesTemplate(dependencies).render()


def is_component_task(task: TaskDefinition) -> bool:
    return task.component is not None


@dataclass
class DependenciesTemplate(Templated):
    template = '''
    {% for a, b in dependencies %}
    {% if '.' in a %}
    {{ a.split('.')[0] }}_task.output.{{ a.split('.')[1] }}.set_destination({{ b }}_task)
    {% else %}
    {{ a }}_task.set_destination({{ b }}_task)
    {% endif %}
    {% endfor %}
    '''
    dependencies: List[Tuple[str, str]]


def extract_dependencies(tasks: Dict[str, TaskDefinition]):
    task_ids = [task_id for task_id, task in tasks.items()]
    return [
        (task.input, task_id)
        for task_id, task
        in tasks.items()
        if task.input is not None and task.input.split('.')[0] in task_ids
    ]


class ImportDefinition(NamedTuple):
    module: Optional[str]
    type: Literal['function', 'component', 'transformation']
    submodule: str


def extract_imports(tasks: Dict[str, TaskDefinition], task_kind: Literal['functions', 'components'] = 'functions') -> List[ImportDefinition]:
    imports = set()
    for task in tasks.values():
        if task_kind == 'functions' and not task.function or task_kind == 'components' and not task.component:
            continue
        if task.function:
            parts = task.function.split('.')
            module, submodule, _ = parts
            imports.add(ImportDefinition(
                module=module if module != 'functions' else None,
                type='function',
                submodule=submodule
            ))
        # Find transformations and append
        for item in task.args.values():
            imports = imports.union(get_transformations_item(item))

    return sorted(imports, key=lambda x: x.submodule)


@dataclass
class ImportsTemplate(Templated):
    template = '''
    {% for import_from, import_as in typhoon_functions_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for import_from, import_as in typhoon_transformations_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for _import in imports %}
    {% if _import.module is none %}
    import {{ _import.type }}s.{{ _import.submodule }}
    {% endif %}
    {% endfor %}
    '''
    imports: List[ImportDefinition]
    _extensions_info: ExtensionsInfo = None

    @property
    def extensions_info(self):
        if self._extensions_info is None:
            self._extensions_info = get_typhoon_extensions_info()
        return self._extensions_info

    @property
    def typhoon_functions_modules(self):
        function_modules_in_use = [x.submodule for x in self.imports if x.type == 'function' and x.module == 'typhoon']
        return [(v, typhoon_import_function_as(k)) for k, v in self.extensions_info['functions'].items() if k in function_modules_in_use]

    @property
    def typhoon_transformations_modules(self):
        transformation_modules_in_use = [x.submodule for x in self.imports if x.type == 'transformation' and x.module == 'typhoon']
        return [(v, typhoon_import_transformation_as(k)) for k, v in self.extensions_info['transformations'].items() if k in transformation_modules_in_use]


def get_transformations_item(item) -> Set[ImportDefinition]:
    if isinstance(item, Py) and ('transformations.' in item.value or 'typhoon.' in item.value):
        import_definitions = set()
        typhoon_transformations = re.findall(r'typhoon\.(\w+)\.\w+', item.value)
        for submodule in typhoon_transformations:
            import_definitions.add(ImportDefinition(
                module='typhoon',
                type='transformation',
                submodule=submodule,
            ))
        custom_transformations = re.findall(r'transformations\.(\w+)\.\w+', item.value)
        for submodule in custom_transformations:
            import_definitions.add(ImportDefinition(
                module=None,
                type='transformation',
                submodule=submodule,
            ))
        return import_definitions
    elif isinstance(item, MultiStep):
        import_definitions = set()
        for x in item.value:
            import_definitions = import_definitions.union(get_transformations_item(x))
        return import_definitions
    elif isinstance(item, list):
        import_definitions = set()
        for x in item:
            import_definitions = import_definitions.union(get_transformations_item(x))
        return import_definitions
    elif isinstance(item, dict):
        import_definitions = set()
        for k, v in item.items():
            import_definitions = import_definitions.union(get_transformations_item(v))
        return import_definitions
    else:
        return set()


def render_args(args: dict) -> str:
    return TaskArgs(args).render()


def typhoon_import_function_as(module_name: str) -> str:
    return f'typhoon_functions_{module_name}'


def typhoon_import_transformation_as(module_name: str) -> str:
    return f'typhoon_transformations_{module_name}'