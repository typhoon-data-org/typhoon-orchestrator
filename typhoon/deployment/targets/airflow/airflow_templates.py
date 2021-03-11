from datetime import datetime
from typing import Union, Optional, List

from dataclasses import dataclass
from typhoon.core.cron_utils import aws_schedule_to_cron, timedelta_from_cron
from typing_extensions import TypedDict

from typhoon.core.dags import DAG, Edge, Node
from typhoon.core.templated import Templated
from typhoon.core.transpiler import get_transformations_modules, get_functions_modules, clean_function_name, \
    clean_simple_param, substitute_special


@dataclass
class AirflowDag(Templated):
    template = '''
    import datetime
    import json
    import os
    import types
    from pathlib import Path
    
    import pendulum
    from airflow import DAG
    from airflow.models import TaskInstance
    {% if airflow_version == 2 %}
    from airflow.operators.python import PythonOperator
    {% else %}
    from airflow.operators.python_operator import PythonOperator
    {% endif %}
    
    TYPHOON_HOME = Path(__file__).parent.parent.parent/'typhoon_extension'
    os.environ['TYPHOON_HOME'] = str(TYPHOON_HOME)
    
    
    import typhoon.contrib.functions as typhoon_functions
    import typhoon.contrib.transformations as typhoon_transformations
    from typhoon.contrib.hooks.hook_factory import get_hook
    
    {{ typhoon_imports }}
    
    
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
        start_date=datetime.datetime.now() - {{ delta }}
    ) as dag:
        {% for dependency in edge_dependencies %}
        {% if dependency.input is none %}
        # Source task {{ dependency.task_id }}
        def {{ dependency.task_id }}_task(**context):
            output = {{ dependency.task_function }}({}, 1, **context)
            context['ti'].xcom_push('result', json.dumps(list(output)))
        {% else %}
        # noinspection DuplicatedCode
        def {{ dependency.task_id }}_task(**context):
            source_task_id = '{{ dependency.input.task_id }}'
            data = json.loads(context['ti'].xcom_pull(task_ids=source_task_id, key='result'))
            result = []
            for batch_num, batch in enumerate(data, 1):
                adapter_config = {}
                {% for adapter in dependency.rendered_adapters %}
                {{ adapter | indent(12, False) }}
                {% endfor %}
                result + list({{ dependency.task_function }}(adapter_config, batch_num, **context))
            context['ti'].xcom_push('result', json.dumps(list(result)))
        {% endif %}
        {{ dependency.task_id }} = PythonOperator(
            task_id='{{ dependency.task_id }}',
            python_callable={{ dependency.task_id }}_task,
            provide_context=True,
        )
        {% if dependency.input is none %}
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

    def __post_init__(self):
        if isinstance(self.dag, dict):
            self.dag = DAG.parse_obj(self.dag)
        if not self.start_date:
            self.start_date = datetime.now()

    @property
    def cron_expression(self):
        return aws_schedule_to_cron(self.dag.schedule_interval)

    @property
    def delta(self):
        return repr(timedelta_from_cron(self.cron_expression))

    @property
    def typhoon_imports(self):
        functions_modules = "\n".join('import ' + x for x in get_functions_modules(self.dag.nodes))
        transformations_modules = "\n".join('import ' + x for x in get_transformations_modules(self.dag))
        return f'{functions_modules}\n\n{transformations_modules}' if transformations_modules else functions_modules

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

        def find_dependencies(inp: Optional[Inp], labels: List[str], node_name: str, prev: str = None):
            node = self.dag.nodes[node_name]
            if not node.asynchronous:
                for edge_name in self.dag.get_edges_for_source(node_name):
                    edge = self.dag.edges[edge_name]
                    find_dependencies(inp, labels, edge.destination, prev=node_name)
                return
            # Asynchronous
            if (prev is None and inp is None) or self.dag.nodes[prev or inp['node_name']].asynchronous:
                task_id = '__'.join([node_name] + labels)
                task_function = f'{node_name}_node'
                is_async = True
            else:   # Async false
                source_node = prev or inp['node_name']
                edge_name = self.dag.get_edge_name(source_node, node_name)
                task_id = '__'.join([f'{edge_name}'] + labels)
                task_function = f'{self.dag.get_edge_name(source_node, node_name)}_sync_edge'
                is_async = False
            rendered_adapters = []
            if inp is not None:
                inbound_edge = self.dag.get_edge(inp['node_name'], prev or node_name)
                for k, v in inbound_edge.adapter.items():
                    rendered_adapters.append(Adapter(k, v, 'adapter_config').render())
            dependencies.append({
                'task_id': task_id,
                'task_function': task_function,
                'input': inp,
                'is_async': is_async,
                'rendered_adapters': rendered_adapters,
            })
            for edge_name in self.dag.get_edges_for_source(node_name):
                edge = self.dag.edges[edge_name]
                find_dependencies(inp={'node_name': node_name, 'task_id': task_id}, labels=labels+[node_name], node_name=edge.destination)

        for source in self.dag.sources:
            find_dependencies(None, [], source)

        return dependencies


@dataclass
class NodeTask(Templated):
    template = '''
    def {{ node_name }}_node(adapter_config: dict, batch_num, **context):
            config = {**adapter_config} 
            {% for adapter in rendered_adapters %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            out = {{ node.function | clean_function_name('functions') }}(**config)
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
            yield Adapter(k, v).render()

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)


@dataclass
class SynchronousEdge(Templated):
    template = '''
    def {{ edge_name }}_sync_edge(adapter_config: dict, batch_num, **context):
        config = {**adapter_config}
        source_data = {{ edge.source }}_node(adapter_config, batch_num, **context)
        for batch_num_dest, batch in enumerate(source_data or [], start=1):
            {% for adapter in rendered_adapters %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            yield from {{ edge.destination }}_node(dest_config, batch_num_dest, **context)
    '''
    edge_name: str
    edge: Edge

    @property
    def rendered_adapters(self):
        for k, v in self.edge.adapter.items():
            yield Adapter(k, v).render()


@dataclass
class Adapter(Templated):
    template = '''
    # Parameter {{ key }}
    {% if '=>APPLY' not in key | replace(' ', '') %}
    {{ config_name }}['{{ key }}'] = {{ value | clean_simple_param }}
    {% else %}
    {% set transforms = value if value is iterable and value is not string else [value] %}
    {% for transform in transforms %}
    {{ key.replace(' ', '').split('=>')[0] }}_{{ loop.index }} = {{ transform | substitute_special(key) | clean_function_name('transformations') }}
    {% if loop.last %}
    {{ config_name }}['{{ key.replace(' ', '').split('=>')[0] }}'] = {{ key.replace(' ', '').split('=>')[0] }}_{{ loop.index }}
    {% endif %}
    {% endfor %}
    {% endif %}
    '''
    key: str
    value: str
    config_name: str = 'config'

    @staticmethod
    def clean_simple_param(x):
        return clean_simple_param(x)

    @staticmethod
    def substitute_special(code, key):
        return substitute_special(code, key, target='airflow')

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)


@dataclass
class AirflowSourceTask(Templated):
    template = '''
    def {{ node_name }}_source(**context):
        config = {} 
        {% for adapter in rendered_adapters %}
        {{ adapter | indent(4, False) }}
        {% endfor %}
        result = json.dumps(list({{ node.function | clean_function_name('functions') }}(**config)))
        context['ti'].xcom_push('result', result)
    '''
    node_name: str
    node: Node

    @property
    def rendered_adapters(self):
        for k, v in self.node.config.items():
            yield Adapter(k, v).render()

    @staticmethod
    def clean_function_name(function_name, function_type):
        return clean_function_name(function_name, function_type)


@dataclass
class AirflowTask(Templated):
    template = '''
    ## Edge {{ edge_name }}: {{ edge.source }} -> {{ edge.destination }}
    def {{ task_name }}(tid, **context):
        config = {} 
        {% for adapter in rendered_adapters_destination %}
        {{ adapter | indent(4, False) }}
        {% endfor %}
        data = json.loads(context['ti'].xcom_pull(task_ids=tid, key='result'))
        result = []
        for batch_num, batch in enumerate(data):
            {% for adapter in rendered_adapters_edge %}
            {{ adapter | indent(8, False) }}
            {% endfor %}
            result += list({{ destination_node.function | clean_function_name('functions') }}(**config))
        context['ti'].xcom_push('result', json.dumps(result))
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
            yield Adapter(k, v).render()

    @property
    def rendered_adapters_edge(self):
        for k, v in self.edge.adapter.items():
            yield Adapter(k, v).render()


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
