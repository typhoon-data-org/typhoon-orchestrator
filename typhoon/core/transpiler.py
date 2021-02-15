from dataclasses import dataclass
from typhoon.core.dags import DAG, Py, MultiStep
from typhoon.core.templated import Templated
from typhoon.core.transpiler_old import clean_simple_param, substitute_special, clean_function_name, \
    get_transformations_modules, get_functions_modules
from typing import Any, List, Tuple


@dataclass
class TyphoonFileTemplate(Templated):
    template = '''
    # DAG name: {{ dag.name }}
    # Schedule interval = {{ dag.schedule_interval }}
    import os
    import types
    from datetime import datetime
    from typing import Dict
    
    import typhoon.contrib.functions as typhoon_functions
    import typhoon.contrib.transformations as typhoon_transformations
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.handler import handle
    from typhoon.core import SKIP_BATCH, task, DagContext, setup_logging
    {% for transformations_module in transformations_modules %}
    
    import {{ transformations_module }}
    {% endfor %}
    {% for functions_module in functions_modules %}
    import {{ functions_module }}
    {% endfor %}
    
    
    from typhoon.core.settings import Settings
    
    
    os.environ['TYPHOON_HOME'] = os.path.dirname(__file__)
    DAG_ID = '{{ dag.name }}'
    
    
    def {{ dag.name }}_main(event, context):
        setup_logging()
        if event.get('type'):     # Async execution
            return handle(event, context)
    
        # Main execution
        dag_context = DagContext(execution_date=event['time'])
    
        {% for source in dag.sources %}
        {{ source }}_branches(dag_context=dag_context)
        {% endfor %}
    
    
    # Branches
    {% for node_name, node in dag.nodes.items() %}
    @task(asynchronous={% if not dev_mode and node.asynchronous %}True{% else %}False{% endif %}, dag_name='{{ dag.name }}')
    def {{ node_name }}_branches(dag_context: DagContext, {% if node_name not in dag.sources%}config, {% endif %}batch_num: int = 0):
        data_generator = {{ node_name }}_node(dag_context=dag_context, {% if node_name not in dag.sources%}config=config, {% endif %}batch_num=batch_num)
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
    
            {% else %}
            pass        # Necessary for the generator to be exhausted since it probably has side effects
    
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
            **config,
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
            'time': '2019-02-05T03:00:00Z'
        }
        example_event_task = {
            'type': 'task',
            'dag_name': '{{ name }}',
            'task_name': '{{ (dag.nodes.keys() | list)[0] }}_branches',
            'trigger': 'dag',
            'attempt': 1,
            'args': [],
            'kwargs': {'dag_context': DagContext(execution_date='2019-02-05T03:00:00Z').dict()},
        }
    
        {{ dag.name }}_main(example_event, None)
    {% endif %}
    '''
    dag: DAG
    debug_mode: bool

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


@dataclass
class AdapterParams(Templated):
    template = '''
    # Parameter {{ key }}
    {% if is_literal %}
    config['{{ key }}'] = {{ value | clean_simple_param }}
    {% elif is_py %}
    config['{{ key }}'] = {{ value }}
    {% else %}
    {{ value }}
    {% endif %}
    '''
    key: str
    value: Any

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
        -
          a: 10
          b: !Py 10 + $BATCH[1]
        - !Py $1 + $2['b']
""", yaml.FullLoader))
    print(TyphoonFileTemplate(dag=dag_definition.make_dag(), debug_mode=True))
