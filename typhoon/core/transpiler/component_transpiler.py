from dataclasses import dataclass

from typhoon.core.components import Component
from typhoon.core.templated import Templated
from typhoon.core.old_transpiler import get_transformations_modules, get_typhoon_transformations_modules, \
    get_functions_modules, get_typhoon_functions_modules
from typhoon.core.old_transpiler.transpiler_helpers import camel_case


@dataclass
class ComponentFile(Templated):
    template = '''
    # Component name: {{ component.name }}
    from types import SimpleNamespace
    from typing import Optional, Any, Type
    
    from typhoon.core import DagContext
    from typhoon.contrib.hooks.hook_factory import get_hook
    from typhoon.runtime import ComponentInterface, BrokerInterface, TaskInterface, ComponentArgs

    {% for transformations_module in component | get_transformations_modules %}
    
    import {{ transformations_module }}
    {% endfor %}
    {% for functions_module in component | get_functions_modules %}
    import {{ functions_module }}
    {% endfor %}
    {% for import_from, import_as in component | get_typhoon_functions_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    {% for import_from, import_as in component | get_typhoon_transformations_modules %}
    import {{ import_from }} as {{ import_as }}
    {% endfor %}
    
    
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
            
            # Tasks
            {% for task_id, task in component.tasks.keys() %}
            {{ task_id }}_task = {{ task_id | camel_case }}Task(
                {% if task.asynchronous %}async_broker{% else %}sync_broker{% endif %},
                parent_component=self,
            )
            
            {% endfor %}
            # Dependencies
            #### TODO ####
            
            self.component_sources = [
                {% for component_source in component_sources %}
                {{component_source}},
                {% endfor %}
            ]
            self.output = SimpleNamespace(**{
                {% for output in component.output %}
                '{{ output }}': {{ output }}_task,
                {% endfor %}
            })

    {% for task_id, task in component.tasks %}
    # TODO
    {% endfor %}
'''
    component: Component
    _filters = [
        get_transformations_modules,
        get_typhoon_transformations_modules,
        get_functions_modules,
        get_typhoon_functions_modules,
        camel_case,
    ]
