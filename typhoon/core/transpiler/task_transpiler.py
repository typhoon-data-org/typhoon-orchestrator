from typing import Dict

from dataclasses import dataclass

from typhoon.core.dags import TaskDefinition
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import expand_function, camel_case, \
    extract_imports, ImportsTemplate, render_args


@dataclass
class TaskTemplate(Templated):
    template = '''
    class {{ task_id | camel_case }}Task(TaskInterface):
        def __init__(self, broker: BrokerInterface, parent_component: Optional[ComponentInterface]=None):
            self.broker = broker
            self.task_id = '{{ task_id }}'
            self.function = {{ task.function | expand_function }}
            self.destinations = []
            self.parent_component = parent_component
    
        def get_args(self, dag_context: DagContext, source: Optional[str], batch_num: int, batch: Any) -> dict:
            {% if inside_component %}
            component_args = self.parent_component.args_class(dag_context, source, batch_num, batch)
            {% endif %}
            args = {}
            {{ task.args | render_args | indent(8, False) }}
            return args
    '''
    task_id: str
    task: TaskDefinition
    inside_component: bool = False
    _filters = [
        expand_function,
        camel_case,
        render_args,
    ]


def render_task(task_id: str, task: TaskDefinition, inside_component) -> str:
    return TaskTemplate(task_id=task_id, task=task, inside_component=inside_component).render()


@dataclass
class TasksFile(Templated):
    template = '''
    from typing import Any, Optional
    
    from typhoon.core.settings import Settings
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.core.runtime import TaskInterface, BrokerInterface, ComponentInterface
    from typhoon.core import DagContext
    
    {{ render_imports }}
    {% for task_id, task in tasks.items() if task.function is not none %}
    
    {{ task_id | render_task(task, inside_component=False) }}
    
    {% endfor %}
    '''
    tasks: Dict[str, TaskDefinition]
    _filters = [render_task]

    @property
    def render_imports(self) -> str:
        imports = extract_imports(self.tasks)
        return ImportsTemplate(imports).render()
