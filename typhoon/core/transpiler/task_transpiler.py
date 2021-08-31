from dataclasses import dataclass
from typing import Dict

from typhoon.core.dags import DAGDefinitionV2, TaskDefinition
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import expand_function, get_transformations_modules, \
    get_typhoon_transformations_modules, get_functions_modules, get_typhoon_functions_modules, camel_case, TaskArgs, \
    extract_imports, ImportsTemplate


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
            component_args = self.parent_component.args_class(source, batch_num, batch)
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
    ]

    @staticmethod
    def render_args(args: dict) -> str:
        return TaskArgs(args).render()


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
    {{ task_id | render_task(task) }}
    {% endfor %}
    '''
    dag: DAGDefinitionV2
    _filters = [
        get_transformations_modules,
        get_typhoon_transformations_modules,
        get_functions_modules,
        get_typhoon_functions_modules,
    ]

    @staticmethod
    def render_task(task_id: str, task: TaskDefinition) -> str:
        return TaskTemplate(task_id=task_id, task=task).render()


def render_task(task_id: str, task: TaskDefinition, inside_component) -> str:
    return TaskTemplate(task_id=task_id, task=task, inside_component=inside_component).render()


@dataclass
class TasksFile(Templated):
    template = '''
    from typing import Any, Optional
    
    from typhoon.core.runtime import TaskInterface, BrokerInterface, ComponentInterface
    from typhoon.core import DagContext
    
    {{ render_imports }}
    {% for task_id, task in tasks.items() %}
    
    {{ task_id | render_task(task, inside_component=False) }}
    
    {% endfor %}
    '''
    tasks: Dict[str, TaskDefinition]
    _filters = [render_task]

    @property
    def render_imports(self) -> str:
        imports = extract_imports(self.tasks)
        return ImportsTemplate(imports).render()
