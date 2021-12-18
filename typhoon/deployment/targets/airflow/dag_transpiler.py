from datetime import datetime
from typing import Any, List, Tuple, Dict

from croniter import croniter
from dataclasses import dataclass

from typhoon.core.cron_utils import aws_schedule_to_cron
from typhoon.core.dags import DAGDefinitionV2, TaskDefinition
from typhoon.core.glue import load_component
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import ImportsTemplate, extract_dependencies, camel_case, extract_imports, render_dependencies, \
    is_component_task, render_args


@dataclass
class AirflowDagFile(Templated):
    template = '''
    # DAG name: {{ dag.name }}
    import datetime
    import pendulum
    from airflow import DAG
    from airflow.models import TaskInstance
    {% if airflow_version == 2 %}
    from airflow.operators.python import PythonOperator
    {% else %}
    from airflow.operators.python_operator import PythonOperator
    {% endif %}
    from typing import Any
    from typhoon.core.settings import Settings
    from typhoon.core import setup_logging, DagContext
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.core.runtime import SequentialBroker, ComponentArgs
    from typhoon.deployment.targets.airflow.runtime import AirflowBroker, make_airflow_tasks
    {% if non_component_tasks %}
    
    from typhoon_managed.{{ dag.name }}.tasks import {% for task_name in non_component_tasks.keys() %}{{ task_name | camel_case }}Task{% if not loop.last %}, {% endif %}{% endfor %}
    
    {% endif %}
    {% for task_name, task in component_tasks.items() %}
    from typhoon_managed.components.{{ task.component.split('.')[-1] }} import {{ task.component.split('.')[-1] | camel_case }}Component
    {% endfor %}

    {{ render_imports }}
    
    
    {% for task_name, task in component_tasks.items() %}
    class {{ task_name | camel_case }}ComponentArgs(ComponentArgs):
        parent_component = None
        
        def __init__(self, dag_context: DagContext, source: str, batch_num: int, batch: Any):
            self.dag_context = dag_context
            self.source = source
            self.batch = batch
            self.batch_num = batch_num
            self._args_cache = None

        def get_args(self) -> dict:
            dag_context = self.dag_context
            batch = self.batch
            batch_num = self.batch_num
            
            {% if task.input %}
            # When multiple inputs are supported add this back
            # if self.source == '{{ task.input }}':
            args = {}
            {{ task.args | render_args | indent(8, False) }}
            {{ task | default_args_not_set | render_args | indent(8, False) }}
            return args
            # assert False, 'Compiler error'
            {% else %}
            args = {}
            {{ task.args | render_args | indent(8, False) }}
            {{ task | default_args_not_set | render_args | indent(8, False) }}
            return args
            {% endif %}
    
    
    {% endfor %}
    sync_broker = SequentialBroker()
    async_broker = AirflowBroker(dag_id='{{ dag.name }}')
    
    # Initialize typhoon tasks
    {% for task_name, task in dag.tasks.items() %}
    {% if task | is_component_task %}
    {{ task_name }}_task = {{ task.component.split('.')[-1] | camel_case }}Component(
        '{{ task_name }}',
        {{ task_name | camel_case }}ComponentArgs,
        sync_broker,
        async_broker,
    )
    {% else %}
    {{ task_name }}_task = {{ task_name | camel_case }}Task(
        {% if task.asynchronous %}
        async_broker,
        {% else %}
        sync_broker,
        {% endif %}
    )
    {% endif %}
    {% endfor %}
    
    {% if dependencies %}
    
    # Set dependencies
    {{ dependencies | render_dependencies }}
    {% endif %}
    
    with DAG(
        dag_id='{{ dag.name }}',
        default_args={'owner': '{{ owner }}'{% if dag.airflow_default_args %}, **{{ dag.airflow_default_args }}{% endif %}},
        schedule_interval='{{ cron_expression }}',
        start_date={{ start_date.__repr__() }},
    ) as dag:
        # Create airflow tasks for each source
        {% for source_task_name in dag.sources.keys() %}
        make_airflow_tasks(dag, {{ source_task_name }}_task, airflow_version={{ airflow_version }})
        {% endfor %}
    
        {% if debug_mode %}
        # Run this in IDE to debug
        if __name__ == '__main__':
            d = pendulum.datetime.now()
            # ti = TaskInstance(dag.task_dict['my_task'], d)
            # ti.run(ignore_all_deps=True, test_mode=True)
            
        {% endif %}
    '''
    dag: DAGDefinitionV2
    start_date: datetime
    debug_mode: bool = False
    airflow_version: int = 1
    owner: str = 'typhoon'
    _filters = [camel_case, render_dependencies, list, is_component_task, render_args]
    _dependencies = None

    def __post_init__(self):
        if isinstance(self.dag, dict):
            self.dag = DAGDefinitionV2.parse_obj(self.dag)
        if not self.start_date:
            cron = aws_schedule_to_cron(self.cron_expression)
            iterator = croniter(cron, datetime.now())
            self.start_date = iterator.get_prev(datetime)
            self.start_date = iterator.get_prev(datetime)
    
    @property
    def render_imports(self) -> str:
        imports = extract_imports(self.dag.tasks, task_kind='components')
        return ImportsTemplate(imports).render()

    @property
    def cron_expression(self):
        return aws_schedule_to_cron(self.dag.schedule_interval)

    @property
    def dependencies(self) -> List[Tuple[str, str]]:
        if self._dependencies is None:
            self._dependencies = extract_dependencies(self.dag.tasks)
        return self._dependencies

    @property
    def non_component_tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.dag.tasks.items() if v.function is not None}

    @property
    def component_tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.dag.tasks.items() if v.component is not None}
    
    @staticmethod
    def default_args_not_set(component_task: TaskDefinition) -> Dict[str, Any]:
        kind, component_name = component_task.component.split('.')
        if kind == 'components':
            kind = 'custom'
        component = load_component(component_name, kind=kind)
        return {
            arg: val.default
            for arg, val in component.args.items()
            if arg not in component_task.args
        }
