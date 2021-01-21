from datetime import datetime
from typing import Union, Optional, List

from dataclasses import dataclass

from typhoon.core.dags import DAG, Edge, Node
from typhoon.core.templated import Templated
from typhoon.core.transpiler import get_functions_modules, get_transformations_modules, clean_simple_param, \
    substitute_special, clean_function_name
from typhoon.deployment.targets.airflow.airflow_cron import aws_schedule_to_airflow_cron, timedelta_from_cron


@dataclass
class AirflowDag(Templated):
    template = '''
    import json
    import datetime
    
    from airflow import DAG
    {% if airflow_version == 2 %}
    from airflow.operators.python import PythonOperator
    {% else %}
    from airflow.operators.python_operator import PythonOperator
    {% endif %}
    
    import typhoon.contrib.functions as typhoon_functions
    import typhoon.contrib.transformations as typhoon_transformations
    from typhoon.contrib.hooks.hook_factory import get_hook
    
    {{ typhoon_imports }}
    
    
    # Source tasks
    {% for source_task in source_tasks %}
    {{ source_task }}
    
    
    {% endfor %}
    # Edge tasks
    {% for edge_task in edge_tasks %}
    {{ edge_task }}
    
    
    {% endfor %}
    with DAG(
        dag_id='{{ dag.name }}',
        default_args={'owner': '{{ owner }}'},
        schedule_interval='{{ cron_expression }}',
        start_date=datetime.now() - {{ delta }}
    ) as dag:
        # Sources
        {% for source in dag.sources %}
        {{ source }} = PythonOperator(
            task_id='{{ source }}',
            python_callable={{ source }}_source,
            provide_context=True,
        )
        dag >> {{ source }}
        
        {% endfor %}
        # Edges
        {% for dependency in edge_dependencies %}
        {{ dependency.task_id }} = PythonOperator(
            task_id='{{ dependency.task_id }}',
            python_callable={{ dependency.task_function }},
            op_kwargs={'tid': '{{ dependency.input }}'},
            provide_context=True,
        )
        {{ dependency.input }} >> {{ dependency.task_id }}
        
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
        return aws_schedule_to_airflow_cron(self.dag.schedule_interval)

    @property
    def delta(self):
        return repr(timedelta_from_cron(self.cron_expression))

    @property
    def typhoon_imports(self):
        functions_modules = "\n".join('import ' + x for x in get_functions_modules(self.dag.nodes))
        transformations_modules = "\n".join('import ' + x for x in get_transformations_modules(self.dag))
        return f'{functions_modules}\n\n{transformations_modules}' if transformations_modules else functions_modules

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
    def edge_dependencies(self):
        dependencies = []

        def find_dependencies(inp: Optional[str], labels: List[str], node: str):
            for edge in self.dag.get_edges_for_source(node):
                task_id = '__'.join([f'{edge.destination}__{edge.source}'] + labels)
                dependencies.append({
                    'task_id': task_id,
                    'task_function': self.dag.get_edge_name(edge.source, edge.destination),
                    'input': inp,
                })
                find_dependencies(inp=task_id, labels=labels+[node], node=edge.destination)

        for source in self.dag.sources:
            find_dependencies(source, [], source)

        return dependencies


@dataclass
class Adapter(Templated):
    template = '''
    # Parameter {{ key }}
    {% if '=>APPLY' not in key | replace(' ', '') %}
    config['{{ key }}'] = {{ value | clean_simple_param }}
    {% else %}
    {% set transforms = value if value is iterable and value is not string else [value] %}
    {% for transform in transforms %}
    {{ key.replace(' ', '').split('=>')[0] }}_{{ loop.index }} = {{ transform | substitute_special(key) | clean_function_name('transformations') }}
    {% if loop.last %}
    config['{{ key.replace(' ', '').split('=>')[0] }}'] = {{ key.replace(' ', '').split('=>')[0] }}_{{ loop.index }}
    {% endif %}
    {% endfor %}
    {% endif %}
    '''
    key: str
    value: str

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
name: telegram_parrot
schedule_interval: 'rate(10 minutes)'

nodes:
  aaa:
    function: functions.testing.echo
    config:
      x: aaa

  bbb:
    function: functions.testing.echo
    config:
      x: bbb

  keep_mine:
    function: typhoon.flow_control.filter
    config:
      filter_func => APPLY: 'lambda x: x.sender_id == 260655064'

  reply:
    function: functions.telegram.send_message_chat
    config:
      hook => APPLY: $HOOK.reminder_bot

edges:
  all_aaa:
    source: aaa
    adapter:
      data => APPLY: $BATCH
    destination: keep_mine
    
  all_bbb:
    source: bbb
    adapter:
      data => APPLY: $BATCH
    destination: keep_mine

  my_messages_to_reply:
    source: keep_mine
    adapter:
      chat_id => APPLY: $BATCH.sender_id
      message => APPLY:
        - typhoon.templates.render(template=$VARIABLE.parrot_message, message=$BATCH.text)
    destination: reply
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
