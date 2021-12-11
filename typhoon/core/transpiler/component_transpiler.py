from typing import List, Tuple, Dict

from dataclasses import dataclass

from typhoon.core.components import Component
from typhoon.core.dags import TaskDefinition
from typhoon.core.templated import Templated
from typhoon.core.transpiler.transpiler_helpers import camel_case, render_dependencies, extract_dependencies, \
    extract_imports, ImportsTemplate, is_component_task, render_args
from typhoon.core.transpiler.task_transpiler import render_task


@dataclass
class ComponentFile(Templated):
    template = '''
    # Component name: {{ component.name }}
    from types import SimpleNamespace
    from typing import Optional, Any, Type
    
    from typhoon.core import DagContext
    from typhoon.core.settings import Settings
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.core.runtime import ComponentInterface, BrokerInterface, TaskInterface, ComponentArgs
    
    {% for task_name, task in component_tasks.items() %}
    from components.{{ task.component.split('.')[-1] }} import {{ task.component.split('.')[-1] | camel_case }}Component
    {% endfor %}

    {{ render_imports }}
    
    
    class {{ component.name | camel_case }}Component(ComponentInterface):
        def __init__(
            self,
            task_id: str,
            args_class: Type[ComponentArgs],
            sync_broker: BrokerInterface,
            async_broker: BrokerInterface,
        ):
            self.task_id = task_id
            self.args_class = args_class
            
            save_self = self
            
            {% for task_name, task in component_tasks.items() %}
            class {{ task_name | camel_case }}ComponentArgs(ComponentArgs):
                parent_component = save_self
                
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
                    
                    component_args = self.parent_component.args_class(dag_context, self.source, batch_num, batch)
                    
                    # When multiple inputs are supported add this back
                    # if self.source == '{{ task.input }}':
                    args = {}
                    {{ task.args | render_args | indent(16, False) }}
                    return args
                    # assert False, 'Compiler error'
            
            
            {% endfor %}
            
            # Tasks
            {% for task_id, task in component.tasks.items() %}
            {% if task | is_component_task %}
            {{ task_id }}_task = {{ task.component.split('.')[-1] | camel_case }}Component(
                '{{ task_name }}',
                {{ task_id | camel_case }}ComponentArgs,
                sync_broker,
                async_broker,
            )
            {% else %}
            {{ task_id }}_task = {{ task_id | camel_case }}Task(
                {% if task.asynchronous %}
                async_broker,
                {% else %}
                sync_broker,
                {% endif %}
                parent_component=self,
            )
            {% endif %}
            
            {% endfor %}
            {% if dependencies %}
            # Dependencies
            {{ dependencies | render_dependencies | indent(8, False) }}
            
            {% endif %}
            self.component_sources = [
                {% for component_source, _ in component.source_tasks.items() %}
                {{ component_source }}_task,
                {% endfor %}
            ]
            self.output = SimpleNamespace(**{
                {% for output in component.output %}
                '{{ output }}': {{ output }}_task,
                {% endfor %}
            })

    {% for task_id, task in non_component_tasks.items() %}
    
    {{ task_id | render_task(task, inside_component=True) }}
    
    {% endfor %}
'''
    component: Component
    airflow_compilation: bool = False
    _filters = [camel_case, render_dependencies, render_task, is_component_task, render_args]
    _dependencies = None

    @property
    def render_imports(self) -> str:
        imports = extract_imports(self.component.tasks)
        return ImportsTemplate(imports).render()

    @property
    def dependencies(self) -> List[Tuple[str, str]]:
        if self._dependencies is None:
            self._dependencies = extract_dependencies(self.component.tasks)
        return self._dependencies

    @property
    def non_component_tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.component.tasks.items() if v.function is not None}

    @property
    def component_tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.component.tasks.items() if v.component is not None}
