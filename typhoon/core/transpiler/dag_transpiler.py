from typing import List, Tuple, Dict

from dataclasses import dataclass

from typhoon.core.dags import DAGDefinitionV2, TaskDefinition
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import extract_dependencies, camel_case, render_dependencies


@dataclass
class DagFile(Templated):
    template = '''
    # DAG name: {{ dag.name }}
    from typhoon.core import setup_logging, DagContext
    from typhoon.core.runtime import SequentialBroker, ComponentArgs
    {% if tasks %}
    
    from tasks import {% for task_name in tasks.keys() %}{{ task_name | camel_case }}Task{% if not loop.last %}, {% endif %}{% endfor %}
    {% endif %}
    
    
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
        {% for task_name, task in tasks.items() %}
        {{ task_name }}_task = {{ task_name | camel_case }}Task(
            {% if task.asynchronous %}
            async_broker,
            {% else %}
            sync_broker,
            {% endif %}
        )
        {% endfor %}
        {% if dependencies %}
        
        # Set dependencies
        {{ dependencies | render_dependencies }}
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
    _filters = [camel_case, render_dependencies, list]
    _dependencies = None

    @property
    def dependencies(self) -> List[Tuple[str, str]]:
        if self._dependencies is None:
            self._dependencies = extract_dependencies(self.dag.tasks)
        return self._dependencies

    @property
    def tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.dag.tasks.items() if v.function is not None}
