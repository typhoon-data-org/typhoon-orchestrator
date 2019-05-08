import json
from ast import literal_eval
from enum import Enum
from io import StringIO
from typing import Optional

import yaml
from dataclasses import dataclass

from typhoon.aws.plumbing import dynamodb_plumbing
from typhoon.core import get_typhoon_config


class VariableError(Exception):
    pass


class VariableType(Enum):
    STRING = 'string'
    JINJA = 'jinja'
    NUMBER = 'number'
    JSON = 'json'
    YAML = 'yaml'


@dataclass
class Variable:
    id: str
    type: VariableType
    contents: str

    def dict_contents(self):
        dc = self.__dict__.copy()
        dc['type'] = dc['type'].value
        return dc

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = VariableType(self.type)

    def get_contents(self):
        if self.type is VariableType.STRING or self.type is VariableType.JINJA:
            return self.contents
        elif self.type is VariableType.NUMBER:
            try:
                num = literal_eval(self.contents)
                if not isinstance(num, int) and not isinstance(num, float):
                    raise VariableError(f'{num} is not a number')
                return num
            except ValueError:
                raise VariableError(f'{self.contents} is not a number')
        elif self.type is VariableType.JSON:
            return json.loads(self.contents)
        elif self.type is VariableType.YAML:
            return yaml.load(StringIO(self.contents), Loader=yaml.FullLoader)
        else:
            assert False


def set_variable(variable: Variable, use_cli_config: bool = False, target_env: Optional[str] = None,):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.dynamodb_put_item(
        ddb_client=config.dynamodb_client,
        table_name=config.variables_table_name,
        item={
            'id': variable.id,
            **variable.dict_contents(),
        }
    )


def get_variable(variable_id: str, use_cli_config: bool = False, target_env: Optional[str] = None,) -> Variable:
    config = get_typhoon_config(use_cli_config, target_env)
    item = dynamodb_plumbing.dynamodb_get_item(
        ddb_client=config.dynamodb_client,
        table_name=config.variables_table_name,
        key_name='id',
        key_value=variable_id,
    )
    return Variable(**item)


def get_variable_contents(variable_id: str, use_cli_config: bool = False, target_env: Optional[str] = None,):
    variable = get_variable(variable_id, use_cli_config, target_env)
    return variable.get_contents()


def delete_variable(variable_id: str, use_cli_config: bool = False, target_env: Optional[str] = None,):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.dynamodb_delete_item(
        ddb_client=config.dynamodb_client,
        table_name=config.variables_table_name,
        key_name='id',
        key_value=variable_id,
    )


def scan_variables(to_dict: bool = False, use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    variables_raw = dynamodb_plumbing.scan_dynamodb_table(
        ddb_resource=config.dynamodb_resource,
        table_name=config.variables_table_name,
    )
    return [Variable(**var).dict_contents() if to_dict else Variable(**var) for var in variables_raw]
