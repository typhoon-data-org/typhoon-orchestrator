import re
from types import SimpleNamespace

from pydantic import BaseModel, Field
from typhoon.core.dags import IDENTIFIER_REGEX, TaskDefinition, Py, MultiStep, add_yaml_constructors
from typing import Dict, List


# from yaml import ScalarNode
#
# ScalarNode.


class ComponentError(Exception):
    pass


def task_name(name_in_dag: str, task: str) -> str:
    return f'_{name_in_dag}__{task}'


def config_arg_name(component_name: str, arg: str) -> str:
    return f'_{component_name}__{arg}'


class Component(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    args: Dict[str, str]
    tasks: Dict[str, TaskDefinition]
    output: List[str]

    def replace_input_and_args(self, task: TaskDefinition, input_task: str, input_arg_values: dict) -> TaskDefinition:
        new_task = task.copy(
            update={
                'input': task.input.replace('$COMPONENT_INPUT', input_task),
                'args': {
                    **{
                        config_arg_name(self.name, k): v
                        for k, v in input_arg_values.items()
                    },
                    **{
                        k: self.replace_args_with_reference(v)
                        for k, v in task.args.items()
                    }
                },
            },
        )
        return new_task

    def replace_args_with_reference(self, item):
        if isinstance(item, Py):
            item.value = re.sub(r'\$ARG\.(\w+)', f"config['_{self.name}__\\1']", item.value)
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
        assert False

    def make_tasks(self, name_in_dag: str, input_task: str, input_arg_values: dict) -> Dict[str, TaskDefinition]:
        return {
            task_name(name_in_dag, k): self.replace_input_and_args(v, input_task, input_arg_values)
            for k, v in self.tasks.items()
        }

    def can_connect(self, task: str) -> bool:
        return task in self.output


if __name__ == '__main__':
    import yaml
    add_yaml_constructors()
    component = Component.parse_obj(yaml.load("""
name: if
args:
    condition: Callable[[T], bool]
    data: T

tasks:
    then:
        input: $COMPONENT_INPUT
        function: typhoon.flow_control.filter
        args:
            filter_func: !Py $ARG.condition
            data: !Py $ARG.data

    else:
        input: $COMPONENT_INPUT
        function: typhoon.flow_control.filter
        args:
            filter_func: !Py  "lambda x: not $ARG.condition(x)"
            data: !Py $ARG.data

output:
    - then
    - else
""", yaml.FullLoader))
    print(yaml.dump(
        {
            k: v.dict()
            for k, v in component.make_tasks(
                name_in_dag='if_mine',
                input_task='read_messages',
                input_arg_values={'condition': Py('lambda x: x == 1'), 'data': '1'}
            ).items()
        },
        sort_keys=False,
    ), yaml.Dumper)
