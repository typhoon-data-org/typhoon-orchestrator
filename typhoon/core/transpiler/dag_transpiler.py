from typing import List, Tuple, Dict, Any

from dataclasses import dataclass

from typhoon.core.components import Component
from typhoon.core.dags import DAGDefinitionV2, TaskDefinition
from typhoon.core.glue import load_component
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import extract_dependencies, camel_case, render_dependencies, \
    is_component_task, render_args, extract_imports, ImportsTemplate


@dataclass
class DagFile(Templated):
    template = '''
    # DAG name: {{ dag.name }}
    from typing import Any
    from typhoon.core.settings import Settings
    from typhoon.core import setup_logging, DagContext
    from typhoon.core.runtime import SequentialBroker, ComponentArgs
    from typhoon.contrib.hooks.hook_factory import get_hook
    {% if non_component_tasks %}
    
    from tasks import {% for task_name in non_component_tasks.keys() %}{{ task_name | camel_case }}Task{% if not loop.last %}, {% endif %}{% endfor %}
    
    {% endif %}
    {% for task_name, task in component_tasks.items() %}
    from components.{{ task.component.split('.')[-1] }} import {{ task.component.split('.')[-1] | camel_case }}Component
    {% endfor %}
    
    {{ render_imports }}
    
    
    {% for task_name, task in component_tasks.items() %}
    class {{ task_name | camel_case }}ComponentArgs(ComponentArgs):
        def __init__(self, dag_context: DagContext, source: str, batch_num: int, batch: Any):
            self.dag_context = dag_context
            self.source = source
            self.batch = batch
            self.batch_num = batch_num
            self.parent_component = None
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
            # assert False, f'Compiler error. Unrecognised source {self.source}'
            {% else %}
            args = {}
            {{ task.args | render_args | indent(8, False) }}
            {{ task | default_args_not_set | render_args | indent(8, False) }}
            return args
            {% endif %}
    
    
    {% endfor %}
    def {{ dag.name }}_main(event, context):
        setup_logging()
        # if event.get('type'):     # TODO: Async execution

        # Main execution
        dag_context = DagContext.from_cron_and_event_time(
            schedule_interval='{{ dag.schedule_interval }}',
            event_time=event['time'],
            granularity='day',
        )
        
        sync_broker = SequentialBroker()
        async_broker = SequentialBroker()
        
        # Initialize tasks
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
        {{ dependencies | render_dependencies | indent(4, False) }}
        {% endif %}
        
        # Sources
        {% for source in dag.sources.keys() %}
        {{ source }}_task.run(dag_context, None, -1, None)
        {% endfor %}
    
    
    if __name__ == '__main__':
        example_event = {
            'time': '2019-02-05T00:00:00Z'
        }
        example_event_task = {
            'type': 'task',
            'dag_name': '{{ dag.name }}',
            'task_name': '{{ (dag.tasks.keys() | list)[0] }}',
            'trigger': 'dag',
            'attempt': 1,
            'args': [],
            'kwargs': {
                'dag_context': DagContext.from_cron_and_event_time(
                    schedule_interval='rate(1 day)',
                    event_time=example_event['time'],
                    granularity='day',
                ).dict()
            },
        }
    
        {{ dag.name }}_main(example_event, None)
    '''
    dag: DAGDefinitionV2
    debug_mode: bool = False
    _filters = [camel_case, render_dependencies, list, is_component_task, render_args]
    _dependencies = None

    @property
    def render_imports(self) -> str:
        imports = extract_imports(self.dag.tasks, task_kind='components')
        return ImportsTemplate(imports).render()

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
        component: Component = load_component(component_name, kind=kind)
        return {
            arg: val.default
            for arg, val in component.args.items()
            if arg not in component_task.args
        }
