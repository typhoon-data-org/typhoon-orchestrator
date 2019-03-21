import json
from ast import literal_eval
from enum import Enum
from io import StringIO
from typing import Optional

import yaml
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from dataclasses import dataclass

from typhoon.aws import connect_dynamodb_metadata, scan_dynamodb_table
from typhoon.settings import get_env


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
        dc = self.__dict__
        dc['type'] = dc['type'].value
        return dc

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = VariableType(self.type)


def set_variable(env: str, variable: Variable):
    ddb = connect_dynamodb_metadata(env, 'client')
    serializer = TypeSerializer()
    ddb.put_item(
        TableName='Variables',
        Item={
            'id': {'S': variable.id},
            **serializer.serialize(variable.dict_contents())['M']
        })


def get_variable(variable_id: str, env: Optional[str] = None) -> Variable:
    env = env or get_env()
    ddb = connect_dynamodb_metadata(env, 'client')
    response = ddb.get_item(
        TableName='Variables',
        Key={'id': {'S': variable_id}}
    )
    if 'Item' not in response:
        raise ValueError(f'Variable {variable_id} is not defined')
    deserializer = TypeDeserializer()
    var = {k: deserializer.deserialize(v) for k, v in response['Item'].items()}
    return Variable(**var)


def get_variable_contents(variable_id: str):
    variable = get_variable(variable_id)
    if variable.type is VariableType.STRING or variable.type is VariableType.JINJA:
        return variable.contents
    elif variable.type is VariableType.NUMBER:
        try:
            num = literal_eval(variable.contents)
            if not isinstance(num, int) and not isinstance(num, float):
                raise VariableError(f'{num} is not a number')
            return num
        except ValueError:
            raise VariableError(f'{num} is not a number')
    elif variable.type is VariableType.JSON:
        return json.loads(variable.contents)
    elif variable.type is VariableType.YAML:
        return yaml.load(StringIO(variable.contents))
    assert False


def delete_variable(env: str, variable_id: str):
    ddb = connect_dynamodb_metadata(env, 'client')
    ddb.delete_item(
        TableName='Variables',
        Key={'id': {'S': variable_id}}
    )


def scan_variables(env):
    variables_raw = scan_dynamodb_table(env, 'Variables')
    return [Variable(**var).dict_contents() for var in variables_raw]


if __name__ == '__main__':
    import os
    os.environ['TYPHOON-ENV'] = 'dev'
    os.environ['TYPHOON_HOME'] = '/Users/biellls/Desktop/typhoon-example'
    a = get_variable_contents("table_names")
    b = 2
