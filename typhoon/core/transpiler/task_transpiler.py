from dataclasses import dataclass

from typhoon.core.dags import DAGDefinitionV2
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import expand_function, get_transformations_modules, \
    get_typhoon_transformations_modules, get_functions_modules, get_typhoon_functions_modules, camel_case, TaskArgs


@dataclass
class TaskFile(Templated):
    template = '''
    from typing import Any, Optional
    
    {% for transformations_module in dag | get_transformations_modules %}
    
    import {{ transformations_module }}
    {% endfor %}
    {% for functions_module in dag | get_functions_modules %}
    import {{ functions_module }}
    {% endfor %}
    {% for import_from, import_as in dag | get_typhoon_functions_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for import_from, import_as in dag | get_typhoon_transformations_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    
    from typhoon.runtime import TaskInterface, BrokerInterface
    from typhoon.core.dags import DagContext
    
    {% for task_id, task in dag.tasks.items() %}
    class {{ task_id | camel_case }}Task(TaskInterface):
        def __init__(self, dag_context: DagContext, broker: BrokerInterface):
            self.dag_context = dag_context
            self.broker = broker
            self.task_id = '{{ task_id }}'
            self.function = {{ task.function | expand_function }}
            self.destinations = []
            self.parent_component = None
    
        def get_args(self, source: Optional[str], batch_num: int, batch: Any) -> dict:
            args = []
            {{ task.args | render_args }}
            return args
    
    {% endfor %}
    '''
    dag: DAGDefinitionV2
    _filters = [
        get_transformations_modules,
        get_typhoon_transformations_modules,
        get_functions_modules,
        get_typhoon_functions_modules,
        expand_function,
        camel_case,
    ]

    @staticmethod
    def render_args(args: dict) -> str:
        return TaskArgs(args).render()


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
    print(TaskFile(dag=dag_definition))
