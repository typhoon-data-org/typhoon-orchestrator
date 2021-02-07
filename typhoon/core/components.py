from pydantic import BaseModel, Field
from typhoon.core.dags import IDENTIFIER_REGEX, NodeDefinitionV2, is_apply
from typing import Dict, List


# from yaml import ScalarNode
#
# ScalarNode.


class ComponentError(Exception):
    pass


def task_name(name_in_dag: str, task: str) -> str:
    return f'_{name_in_dag}__{task}'


class Component(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    args: Dict[str, str]
    tasks: Dict[str, NodeDefinitionV2]
    output: List[str]

    def replace_input_and_args(self, task: NodeDefinitionV2, input_task: str,
                               input_arg_values: dict) -> NodeDefinitionV2:
        return task.copy(
            update={
                'input': task.input.replace('$COMPONENT_INPUT', input_task),
                'args': {
                    k: self.replace_args_with_values(v, input_arg_values) if is_apply(k) else v
                    for k, v in task.args.items()
                }
            },
        )

    def replace_args_with_values(self, item, input_arg_values: dict):
        if isinstance(item, str):
            for k, v in input_arg_values.items():
                if k not in self.args.keys():
                    raise ComponentError(f'{k} is not an accepted argument for {self.name}')
                argument = f'$ARG.{k}'
                if argument in item:
                    item = item.replace(argument, v)
            return item
        elif isinstance(item, list):
            return [self.replace_args_with_values(x, input_arg_values) for x in item]
        elif isinstance(item, dict):
            return {k: self.replace_args_with_values(v, input_arg_values) for k, v in item.items()}
        assert False

    def make_tasks(self, name_in_dag: str, input_task: str, input_arg_values: dict) -> Dict[str, NodeDefinitionV2]:
        return {
            task_name(name_in_dag, k): self.replace_input_and_args(v, input_task, input_arg_values)
            for k, v in self.tasks.items()
        }

    def can_connect(self, task: str) -> bool:
        return task in self.output


if __name__ == '__main__':
    import yaml
    component = Component.parse_obj(yaml.safe_load("""
name: if
args:
    condition: Callable[[T], bool]
    data: T

tasks:
    then:
        input: $COMPONENT_INPUT
        function: typhoon.flow_control.filter
        args:
            filter_func => APPLY: $ARG.condition
            data => APPLY: $ARG.data

    else:
        input: $COMPONENT_INPUT
        function: typhoon.flow_control.filter
        args:
            filter_func => APPLY: "lambda x: not $ARG.condition(x)"
            data => APPLY: $ARG.data

output:
    - then
    - else
"""))
    print(yaml.safe_dump(
        {
            k: v.dict()
            for k, v in component.make_tasks('if_mine', 'read_messages', {'condition': 'lambda x: x == 1', 'data': '1'}).items()
        },
        sort_keys=False,
    ))
