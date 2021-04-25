import re
from typing import Any, List, Iterable, Dict, Union, Optional

from dataclasses import dataclass
from typhoon.core.dags import DAG, Py, MultiStep, Node
from typhoon.core.templated import Templated
from typhoon.introspection.introspect_extensions import get_typhoon_extensions_info, ExtensionsInfo


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


def get_transformations_modules(dag: DAG) -> Iterable[str]:
    if isinstance(dag, dict):
        dag = DAG.parse_obj(dag)
    modules = set()
    for edge in dag.edges.values():
        for val in edge.adapter.values():
            mods = get_transformations_item(val)
            modules = modules.union(mods)
    return list(modules)


def get_typhoon_transformations_modules(dag: DAG) -> Iterable[str]:
    if isinstance(dag, dict):
        dag = DAG.parse_obj(dag)
    items = set()
    for edge in dag.edges.values():
        for val in edge.adapter.values():
            items = items.union(x.split('.')[0] for x in get_typhoon_transformations_item(val))
    return list(items)


def get_functions_modules(nodes: Dict[str, Node]) -> Iterable[str]:
    modules = set()
    for node in nodes.values():
        if not node.function.startswith('typhoon.'):
            modules.add('.'.join(node.function.split('.')[:-1]))

    return list(modules)


def get_typhoon_functions_modules(nodes: Dict[str, Node]) -> Iterable[str]:
    modules = set()
    for node in nodes.values():
        if node.function.startswith('typhoon.'):
            modules.add(node.function.split('.')[1])

    return list(modules)


def clean_function_name(function_name: str, function_type: str) -> str:
    if not function_name.startswith('typhoon.'):
        return function_name
    else:
        _, module_name, function_name = function_name.split('.')
        return f'typhoon_{function_type}_{module_name}.{function_name}'


def clean_simple_param(param: Union[str, int, float, List, dict]):
    if not isinstance(param, str):
        return param
    return f'"""{param}"""' if "'" in param else f"'{param}'"


def substitute_special(code: str, key: str, target: str = 'typhoon') -> str:
    if '=>' in key:
        key = key.replace(' ', '').split('=>')[0]
    code = code.replace('$BATCH', 'data' if target == 'typhoon' else 'batch')
    code = re.sub(r'\$DAG_CONTEXT(\.(\w+))', r'dag_context.\g<2>', code)
    code = code.replace('$DAG_CONTEXT', 'dag_context')
    code = re.sub(r'\$(\d)+', r"{key}_\g<1>".format(key=key), code)
    code = code.replace('$BATCH_NUM', 'batch_num')
    code = re.sub(r'\$HOOK(\.(\w+))', r'get_hook("\g<2>")', code)
    code = re.sub(r'\$VARIABLE(\.(\w+))', r'Settings.metadata_store().get_variable("\g<2>").get_contents()', code)
    return code


def typhoon_import_function_as(module_name: str) -> str:
    return f'typhoon_functions_{module_name}'


def typhoon_import_transformation_as(module_name: str) -> str:
    return f'typhoon_transformations_{module_name}'


@dataclass
class TyphoonFileTemplate(Templated):
    template = '''
    # DAG name: {{ dag.name }}
    # Schedule interval = {{ dag.schedule_interval }}
    import os
    import types
    import jinja2
    from datetime import datetime
    from typing import Dict
    
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.handler import handle
    from typhoon.core import SKIP_BATCH, task, DagContext, setup_logging, interval_start_from_schedule_and_interval_end
    {% for transformations_module in transformations_modules %}
    
    import {{ transformations_module }}
    {% endfor %}
    {% for functions_module in functions_modules %}
    import {{ functions_module }}
    {% endfor %}
    {% for import_from, import_as in typhoon_functions_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for import_from, import_as in typhoon_transformations_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    
    
    from typhoon.core.settings import Settings
    
    
    os.environ['TYPHOON_HOME'] = os.path.dirname(__file__)
    DAG_ID = '{{ dag.name }}'
    
    
    def {{ dag.name }}_main(event, context):
        setup_logging()
        if event.get('type'):     # Async execution
            return handle(event, context)
    
        # Main execution
        dag_context = DagContext.from_cron_and_event_time(
            schedule_interval='{{ dag.schedule_interval }}',
            event_time=event['time'],
            granularity='{{ dag.granularity.value }}',
        )
    
        {% for source in dag.sources %}
        {{ source }}_branches(dag_context=dag_context)
        {% endfor %}
    
    
    # Branches
    {% for node_name, node in dag.nodes.items() %}
    @task(asynchronous={% if not dev_mode and node.asynchronous %}True{% else %}False{% endif %}, dag_name='{{ dag.name }}')
    def {{ node_name }}_branches(dag_context: DagContext, {% if node_name not in dag.sources%}config, {% endif %}batch_num: int = 0):
        data_generator = {{ node_name }}_node(dag_context=dag_context, {% if node_name not in dag.sources%}config=config.copy(), {% endif %}batch_num=batch_num)
        for batch_num, batch in enumerate(data_generator or [], start=1):
            if batch is SKIP_BATCH:
                print(f'Skipping batch {batch_num} for {{ node_name }}')
                continue
    
            {% for out_node in dag.out_nodes(node_name) %}
            config = {}
            {% for k, v in dag.get_edge(node_name, out_node).adapter.items() %}
            {{ k | adapter_params(v) | indent(8, False) }}
            {% endfor %}
            {{ out_node }}_branches(dag_context=dag_context, config=config, batch_num=batch_num)
    
            {% endfor %}
    
    {% endfor %}
    # Nodes
    {% for node_name, node in dag.nodes.items() %}
    def {{ node_name }}_node(dag_context: DagContext, {% if node_name not in dag.sources %}config: Dict, {% endif %}batch_num: int):
        {% if node_name in dag.sources%}
        config = {}
    
        {% endif %}
        {% for k, v in node.config.items() %}
        {{ k | adapter_params(v) | indent(4, False) }}
        {% endfor %}
        out = {{ node['function'] | clean_function_name('functions') }}(
            **{k: v for k, v in config.items() if not k.startswith('_')},   # Remove component args from config
        )
        if isinstance(out, types.GeneratorType):
            yield from out
        else:
            yield out
    
    
    {% endfor %}
    {% if debug_mode %}
    if __name__ == '__main__':
        import os
    
        example_event = {
            'time': '2019-02-05T00:00:00Z'
        }
        example_event_task = {
            'type': 'task',
            'dag_name': '{{ name }}',
            'task_name': '{{ (dag.nodes.keys() | list)[0] }}_branches',
            'trigger': 'dag',
            'attempt': 1,
            'args': [],
            'kwargs': {
                'dag_context': DagContext.from_cron_and_event_time(
                    schedule_interval='{{ dag.schedule_interval }}',
                    event_time=example_event['time'],
                    granularity='{{ dag.granularity.value }}',
                ).dict()
            },
        }
    
        {{ dag.name }}_main(example_event, None)
    {% endif %}
    '''
    dag: DAG
    debug_mode: bool
    _extensions_info: ExtensionsInfo = None

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)

    @property
    def transformations_modules(self):
        return get_transformations_modules(self.dag)

    @property
    def functions_modules(self):
        return get_functions_modules(self.dag.nodes)

    @staticmethod
    def adapter_params(k, v):
        return AdapterParams(k, v).render()

    @property
    def extensions_info(self):
        if self._extensions_info is None:
            self._extensions_info = get_typhoon_extensions_info()
        return self._extensions_info

    @property
    def typhoon_functions_modules(self):
        function_modules_in_use = get_typhoon_functions_modules(self.dag.nodes)
        return [(v, typhoon_import_function_as(k)) for k, v in self.extensions_info['functions'].items() if k in function_modules_in_use]

    @property
    def typhoon_transformations_modules(self):
        transformation_modules_in_use = get_typhoon_transformations_modules(self.dag)
        return [(v, typhoon_import_transformation_as(k)) for k, v in self.extensions_info['transformations'].items() if k in transformation_modules_in_use]


@dataclass
class AdapterParams(Templated):
    template = '''
    {% if is_literal %}
    {{ config_name }}['{{ key }}'] = {{ value | clean_simple_param }}
    {% elif is_py %}
    {{ config_name }}['{{ key }}'] = {{ value }}
    {% else %}
    {{ value }}
    {% endif %}
    '''
    key: str
    value: Any
    config_name: str = 'config'

    def __post_init__(self):
        if isinstance(self.value, MultiStep):
            self.value.config_name = self.config_name

    @property
    def is_literal(self):
        return not isinstance(self.value, (Py, MultiStep))

    @property
    def is_py(self):
        return isinstance(self.value, Py)

    @staticmethod
    def clean_simple_param(x):
        return clean_simple_param(x)

    @staticmethod
    def substitute_special(code, key):
        return substitute_special(code, key)


if __name__ == '__main__':
    import yaml
    from typhoon.core.dags import DAGDefinitionV2, add_yaml_constructors
    add_yaml_constructors()
    dag_definition = DAGDefinitionV2.parse_obj(yaml.load("""
name: test
schedule_interval: rate(10 minutes)

tasks:
  tables:
    function: typhoon.flow_control.branch
    args:
      branches:
        - [foo, 1]
        - [bar, 2]
        - [baz, 3]

  write_table:
    input: tables
    function: typhoon.echo.tables
    args:
      hook: !Py $HOOK.data_lake
      table: !Py $BATCH[0]
      meta: write_tables
      data: !MultiStep
        - !Py $BATCH[1] + 2
        - !Py transformations.data.to_bytes_buffer
        -
          a: 10
          b: !Py 10 + $BATCH[1]
        - !Py $1 + $2['b']
""", yaml.FullLoader))
    print(TyphoonFileTemplate(dag=dag_definition.make_dag(), debug_mode=True))
