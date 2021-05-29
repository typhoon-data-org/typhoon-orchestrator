import re
from datetime import datetime
from typing import Union, Optional

from croniter import croniter
from dataclasses import dataclass
from typhoon.core.cron_utils import aws_schedule_to_cron, timedelta_from_cron
from typhoon.core.settings import Settings
from typing_extensions import TypedDict

from typhoon.core.dags import DAG, Edge, Node, Py, MultiStep, evaluate_item
from typhoon.core.templated import Templated
from typhoon.core.transpiler import get_transformations_modules, get_functions_modules, clean_function_name, \
    clean_simple_param, substitute_special, AdapterParams, get_typhoon_functions_modules, typhoon_import_function_as, \
    get_typhoon_transformations_modules, typhoon_import_transformation_as
from typhoon.introspection.introspect_extensions import get_typhoon_extensions_info, ExtensionsInfo


def replace_batch_and_batch_num(item, batch, batch_num):
    if isinstance(item, Py):
        return Py(
            value=item.value.replace('$BATCH_NUM', str(batch_num)).replace('$BATCH', batch.__repr__()),
            key=item.key,
            args_dependencies=item.args_dependencies,
        )
    elif isinstance(item, MultiStep):
        return MultiStep(
            value=[replace_batch_and_batch_num(x, batch, batch_num) for x in item.value],
            key=item.key,
            config_name=item.config_name,
        )
    elif isinstance(item, list):
        return [replace_batch_and_batch_num(x, batch, batch_num) for x in item]
    elif isinstance(item, dict):
        return {k: replace_batch_and_batch_num(v, batch, batch_num) for k, v in item.items()}
    return item


@dataclass
class AirflowDag(Templated):
    template = '''
    import datetime
    import os
    import types
    from pathlib import Path
    
    import pendulum
    import airflow
    from airflow import DAG
    from airflow.models import TaskInstance
    {% if airflow_version == 2 %}
    from airflow.operators.python import PythonOperator
    {% else %}
    from airflow.operators.python_operator import PythonOperator
    {% endif %}
    
    
    from typhoon.core import DagContext
    from typhoon.contrib.hooks.hook_factory import get_hook
    
    {% for import_from, import_as in typhoon_functions_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for import_from, import_as in typhoon_transformations_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    
    
    def make_typhoon_dag_context(context):
        interval_start = (context['dag_run'] and (context['dag_run'].conf or {}).get('interval_start')) or context['execution_date']
        interval_end = (context['dag_run'] and (context['dag_run'].conf or {}).get('interval_end')) or context['next_execution_date']
        dag_context = DagContext(interval_start=interval_start, interval_end=interval_end)
        return dag_context
    
    
    # Nodes
    {% for node in nodes %}
    {{ node }}
    
    
    {% endfor %}
    {% for synchronous_edge in synchronous_edges %}
    {{ synchronous_edge }}
    
    
    {% endfor %}
    with DAG(
        dag_id='{{ dag.name }}',
        default_args={'owner': '{{ owner }}'},
        schedule_interval='{{ cron_expression }}',
        start_date={{ start_date.__repr__() }}
    ) as dag:
        {% for dependency in edge_dependencies %}
        {% if dependency.input is none %}
        # Source task {{ dependency.task_id }}
        def {{ dependency.task_id }}_task(**context):
            output = {{ dependency.task_function }}({}, 1, **context)
            context['ti'].xcom_push('result', list(output))
        {% else %}
        # noinspection DuplicatedCode
        def {{ dependency.task_id }}_task(**context):
            dag_context = DagContext(interval_start=context['execution_date'], interval_end=context['next_execution_date'])
            source_task_id = '{{ dependency.input.task_id }}'
            data = context['ti'].xcom_pull(task_ids=source_task_id, key='result')
            result = []
            for batch_num, batch in enumerate(data, 1):
                config = {}
                {% for adapter in dependency.rendered_adapters %}
                {{ adapter | indent(12, False) }}
                {% endfor %}
                result = result + list({{ dependency.task_function }}(config, batch_num, **context))
            context['ti'].xcom_push('result', list(result))
        {% endif %}
        {{ dependency.task_id }} = PythonOperator(
            task_id='{{ dependency.task_id }}',
            python_callable={{ dependency.task_id }}_task,
            provide_context=True,
        )
        {% if dependency.input is none %}
        if airflow.__version__.startswith('1.'):
            dag >> {{ dependency.task_id }}
        {% else %}
        {{ dependency.input.task_id }} >> {{ dependency.task_id }}
        {% endif %}
        
        {% endfor %}
        if __name__ == '__main__':
            d = pendulum.datetime.now()
            {% for dependency in edge_dependencies %}
            ti = TaskInstance({{ dependency.task_id }}, d)
            ti.run(ignore_all_deps=True, test_mode=True)
            
            {% endfor %}
    '''
    dag: Union[DAG, dict]
    owner: str = 'typhoon'
    start_date: Optional[datetime] = None
    airflow_version: int = 1
    _extensions_info: ExtensionsInfo = None

    def __post_init__(self):
        if isinstance(self.dag, dict):
            self.dag = DAG.parse_obj(self.dag)
        if not self.start_date:
            cron = aws_schedule_to_cron(self.cron_expression)
            iterator = croniter(cron, datetime.now())   # In case the event is exactly on time
            self.start_date = iterator.get_prev(datetime)

        # Validate that there's no node with two inputs
        visited = set()
        for e in self.dag.edges.values():
            if e.destination in visited:
                raise ValueError(f'Cannot build airflow DAG. Node {e.destination} has two or more inputs')
            visited.add(e.destination)

        # If there's a branch at the start then explicitly create separate branches for each
        def recursive_branch(node_name, branch_name, new_config=None):
            new_source_node_name = f'{node_name}_{branch_name}'
            new_node = self.dag.nodes[node_name].copy(deep=True)
            if new_config:
                new_node.config.update(**new_config)
            self.dag.nodes[new_source_node_name] = new_node
            for edge_name in self.dag.get_edges_for_source(node_name):
                edge = self.dag.edges[edge_name]
                new_edge = edge.copy()
                new_edge.source = new_source_node_name
                new_edge.destination = f'{edge.destination}_{branch_name}'
                self.dag.edges[f'{edge_name}_{branch_name}'] = new_edge
                recursive_branch(edge.destination, branch_name)

        def recursive_delete(node_name):
            for edge_name in self.dag.get_edges_for_source(node_name):
                edge = self.dag.edges[edge_name]
                recursive_delete(edge.destination)
                del self.dag.edges[edge_name]
            del self.dag.nodes[node_name]

        def dict_with_name(x):
            return isinstance(x, dict) and x.get('name', False)

        def name(branch):
            text = branch if isinstance(branch, str) else branch['name']
            illegal_chars = r' $\'",-'
            for c in illegal_chars:
                text = text.replace(c, '_')
            return text

        for node_name in self.dag.sources:
            node = self.dag.nodes[node_name]
            if isinstance(node.config['branches'], Py):
                custom_locals = {'config': {}}
                for dep in node.config['branches'].args_dependencies:
                    custom_locals['config'][dep] = node.config[dep]
                try:
                    new_value = evaluate_item(custom_locals, node.config['branches'])
                    node.config['branches'] = new_value
                    # for dep in node.config['branches'].args_dependencies:
                    #     del node.config[dep]
                except Exception:
                    print('Can not evaluate {node.config["branches"]}')
            if node.function == 'typhoon.flow_control.branch' and \
                    isinstance(node.config['branches'], list) and \
                    'branches' in node.config and all(isinstance(x, str) or dict_with_name(x) for x in node.config['branches']):
                # Reshape the dag to separate branches
                for edge_name in self.dag.get_edges_for_source(node_name):
                    edge = self.dag.edges[edge_name]
                    for batch_num, branch in enumerate(node.config['branches'], start=1):
                        new_config = replace_batch_and_batch_num(edge.adapter, branch, batch_num)
                        dest = edge.destination
                        recursive_branch(dest, name(branch), new_config)
                recursive_delete(node_name)

    @property
    def typhoon_home(self) -> str:
        return Settings.typhoon_home

    @property
    def cron_expression(self):
        return aws_schedule_to_cron(self.dag.schedule_interval)

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

    @property
    def nodes(self):
        for node_name, node in self.dag.nodes.items():
            yield NodeTask(node_name, node)

    @property
    def source_tasks(self):
        for name, node in self.dag.nodes.items():
            if name not in self.dag.sources:
                continue
            yield AirflowSourceTask(node_name=name, node=node).render()

    @property
    def edge_tasks(self):
        for name in self.dag.edges.keys():
            yield AirflowTask(dag=self.dag, edge_name=name).render()

    @property
    def synchronous_edges(self):
        for edge_name, edge in self.dag.edges.items():
            if not self.dag.nodes[edge.source].asynchronous:
                yield SynchronousEdge(edge_name, edge).render()

    @property
    def edge_dependencies(self):
        dependencies = []

        class Inp(TypedDict):
            node_id: str
            node_name: str

        def find_dependencies(inp: Optional[Inp], node_name: str, prev: str = None):
            node = self.dag.nodes[node_name]
            if not node.asynchronous:
                for edge_name in self.dag.get_edges_for_source(node_name):
                    edge = self.dag.edges[edge_name]
                    find_dependencies(inp, edge.destination, prev=node_name)
                return
            # Asynchronous
            if (prev is None and inp is None) or self.dag.nodes[prev or inp['node_name']].asynchronous:
                task_id = node_name
                task_function = f'{node_name}_node'
                is_async = True
            else:   # Async false
                source_node = prev or inp['node_name']
                edge_name = self.dag.get_edge_name(source_node, node_name)
                task_id = f'{source_node}_then_{node_name}'
                task_function = f'{task_id}_sync_edge'
                is_async = False
            rendered_adapters = []
            if inp is not None:
                inbound_edge = self.dag.get_edge(inp['node_name'], prev or node_name)
                for k, v in inbound_edge.adapter.items():
                    rendered_adapters.append(AdapterParams(k, v).render())
            dependencies.append({
                'task_id': task_id,
                'task_function': task_function,
                'input': inp,
                'is_async': is_async,
                'rendered_adapters': rendered_adapters,
            })
            for edge_name in self.dag.get_edges_for_source(node_name):
                edge = self.dag.edges[edge_name]
                find_dependencies(inp={'node_name': node_name, 'task_id': task_id}, node_name=edge.destination)

        for source in self.dag.sources:
            find_dependencies(None, source)

        return dependencies


@dataclass
class NodeTask(Templated):
    template = '''
    def {{ node_name }}_node(adapter_config: dict, batch_num, **context):
            dag_context = make_typhoon_dag_context(context)
            config = {**adapter_config} 
            {% for adapter in rendered_adapters %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            out = {{ node.function | clean_function_name('functions') }}(**{k: v for k, v in config.items() if not k.startswith('_')})
            if isinstance(out, types.GeneratorType):
                yield from out
            else:
                yield out
    '''
    node_name: str
    node: Node

    @property
    def rendered_adapters(self):
        for k, v in self.node.config.items():
            yield AdapterParams(k, v).render()

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)


@dataclass
class SynchronousEdge(Templated):
    template = '''
    def {{ edge.source }}_then_{{ edge.destination }}_sync_edge(adapter_config: dict, batch_num, **context):
        dag_context = make_typhoon_dag_context(context)
        source_data = {{ edge.source }}_node(adapter_config, batch_num, **context)
        for batch_num, batch in enumerate(source_data or [], start=1):
            config = {}
            {% for adapter in rendered_adapters %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            yield from {{ edge.destination }}_node(config, batch_num, **context)
    '''
    edge_name: str
    edge: Edge

    @property
    def rendered_adapters(self):
        for k, v in self.edge.adapter.items():
            yield AdapterParams(k, v).render()




@dataclass
class AirflowSourceTask(Templated):
    template = '''
    def {{ node_name }}_source(**context):
        dag_context = DagContext(interval_start=context['execution_date'], interval_end=context['next_execution_date'])
        config = {} 
        {% for adapter in rendered_adapters %}
        {{ adapter | indent(4, False) }}
        {% endfor %}
        result = list({{ node.function | clean_function_name('functions') }}(**{k: v for k, v in config.items() if not k.startswith('_')}))
        context['ti'].xcom_push('result', result)
    '''
    node_name: str
    node: Node

    @property
    def rendered_adapters(self):
        for k, v in self.node.config.items():
            yield AdapterParams(k, v).render()

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)


@dataclass
class AirflowTask(Templated):
    template = '''
    ## Edge {{ edge_name }}: {{ edge.source }} -> {{ edge.destination }}
    def {{ task_name }}(tid, **context):
        dag_context = DagContext(interval_start=context['execution_date'], interval_end=context['next_execution_date'])
        config = {} 
        {% for adapter in rendered_adapters_destination %}
        {{ adapter | indent(4, False) }}
        {% endfor %}
        data = context['ti'].xcom_pull(task_ids=tid, key='result')
        result = []
        for batch_num, batch in enumerate(data):
            {% for adapter in rendered_adapters_edge %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            result += list({{ destination_node.function | clean_function_name('functions') }}(**{k: v for k, v in config.items() if not k.startswith('_')}))
        context['ti'].xcom_push('result', result)
    '''
    dag: DAG
    edge_name: str

    @property
    def edge(self):
        return self.dag.edges[self.edge_name]

    @property
    def destination_node(self):
        return self.dag.nodes[self.edge.destination]

    @property
    def task_name(self):
        return self.edge_name

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)

    @property
    def rendered_adapters_destination(self):
        for k, v in self.destination_node.config.items():
            yield AdapterParams(k, v).render()

    @property
    def rendered_adapters_edge(self):
        for k, v in self.edge.adapter.items():
            yield AdapterParams(k, v).render()


if __name__ == '__main__':
    import yaml

    dag_source = """
name: test_ftp
schedule_interval: rate(2 hours)
nodes:
  one:
    function: typhoon.filesystem.list_directory
    config:
      hook => APPLY: $HOOK.ftp
      path: dump/
      
  two:
    function: typhoon.filesystem.list_directory
    config:
      hook => APPLY: $HOOK.ftp2
      path: dump/

  read:
    function: typhoon.filesystem.read_data
    asynchronous: false
    config:
      hook => APPLY: $HOOK.ftp

  s3_upload:
    function: typhoon.filesystem.write_data
    config:
      hook => APPLY: $HOOK.data_lake

  copy_snowflake:
    function: typhoon.filesystem.write_data
    config:
      hook => APPLY: $HOOK.snowflake

edges:
  file_paths_one:
    source: one
    destination: read
    adapter:
      path => APPLY: $BATCH
      abc => '/tmp/' + $BATCH

  file_paths_two:
    source: two
    destination: read
    adapter:
      path => APPLY: $BATCH

  ftp_file_to_s3:
    source: read
    destination: s3_upload
    adapter:
      data => APPLY: $BATCH.data
      path => APPLY:
        - transformations.os.filename($BATCH.path)
        - f'data/{$1}'

  to_snowflake:
    source: s3_upload
    destination: copy_snowflake
    adapter:
      table: 'abc'
      stage_name: 'my_stage'
      s3_path => APPLY: $BATCH
    """

    # test_dag = DAG(
    #     name='test_airflow_dag',
    #     schedule_interval='@daily',
    #     nodes={
    #         'n1': Node(function='typhoon.filesystem.ls'),
    #         'n2': Node(function='functions.telegram.read_message')
    #     },
    #     edges={
    #         'e1': Edge(source='n1', destination='n2', adapter={'x': 'foo', 'y': ['foo', 'bar']})
    #     }
    # )
    test_dag = yaml.safe_load(dag_source)
    rendered = AirflowDag(dag=test_dag).render()
    print(rendered)
