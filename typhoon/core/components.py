import re
from copy import deepcopy
from types import SimpleNamespace
from typing import Dict, List, Union, Any, Optional

from pydantic import BaseModel, Field

from typhoon.core.dags import IDENTIFIER_REGEX, TaskDefinition, Py, MultiStep, evaluate_item


def task_name(name_in_dag: str, task: str) -> str:
    return f'_{name_in_dag}__{task}'


def config_arg_name(component_name: str, arg: str) -> str:
    return f'_{component_name}__{arg}'


class ComponentArgument(BaseModel):
    type: str = Field(..., description='Type of your argument')
    default: Any = Field(..., description='Default value for your argument')
    description: Optional[str] = Field(None, description='Documentation for the argument')


class Component(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your component')
    description: Optional[str] = Field(None, description='Description of what the component does for documentation purposes')
    args: Dict[str, Union[str, ComponentArgument]]
    tasks: Dict[str, TaskDefinition]
    output: List[str] = ()

    def replace_input_and_args(self, name_in_dag: str, task: TaskDefinition, input_task: str, input_arg_values: dict) -> TaskDefinition:
        if task.input == '$COMPONENT_INPUT':
            inp = input_task
        elif isinstance(task.input, str):
            inp = task_name(name_in_dag, task.input)
        else:
            inp = input_task
        component_config = {
            config_arg_name(self.name, k): v
            for k, v in input_arg_values.items()
        }
        component_config = {
            **component_config,
            **{
                config_arg_name(self.name, k): evaluate_item({}, v.default)
                for k, v in self.args.items()
                if k not in input_arg_values.keys()
            }
        }
        new_task = task.copy(
            update={
                'input': inp,
                'args': {
                    **component_config,
                    **{
                        k: self.replace_args_with_reference(deepcopy(v))
                        for k, v in task.args.items()
                    }
                },
            },
        )
        return new_task

    def replace_args_with_reference(self, item):
        if isinstance(item, Py):
            regex = r'\$ARG\.(\w+)'
            item.args_dependencies = [f"_{self.name}__{dep}" for dep in re.findall(regex, item.value)] or None
            item.value = re.sub(regex, f"config['_{self.name}__\\1']", item.value)
            return item
        elif isinstance(item, MultiStep):
            for step in item.value:
                self.replace_args_with_reference(step)
            return item
        elif isinstance(item, SimpleNamespace):
            for v in item.__dict__.values():
                self.replace_args_with_reference(v)
            return item
        elif isinstance(item, list):
            return [self.replace_args_with_reference(x) for x in item]
        elif isinstance(item, dict):
            return {k: self.replace_args_with_reference(v) for k, v in item.items()}
        return item

    def make_tasks(self, name_in_dag: str, input_task: str, input_arg_values: dict) -> Dict[str, TaskDefinition]:
        return {
            task_name(name_in_dag, k): self.replace_input_and_args(name_in_dag, v, input_task, input_arg_values)
            for k, v in self.tasks.items()
        }

    def can_connect(self, task: str) -> bool:
        return task in self.output

    @property
    def source_tasks(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.tasks.items() if v.input is None or v.input == '$COMPONENT_INPUT'}
