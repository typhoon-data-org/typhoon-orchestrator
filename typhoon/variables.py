from enum import Enum

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from dataclasses import dataclass

from typhoon.aws import connect_dynamodb_metadata, scan_dynamodb_table


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
        contents = self.__dict__
        contents.pop('id')
        return contents


def set_variable(env: str, variable: Variable):
    ddb = connect_dynamodb_metadata(env, 'client')
    serializer = TypeSerializer()
    ddb.put_item(
        TableName='Variables',
        Item={
            'id': {'S': variable.id},
            **serializer.serialize(variable.dict_contents())['M']
        })


def get_variable(env: str, variable_id: str) -> Variable:
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


def delete_variable(env: str, variable_id: str):
    ddb = connect_dynamodb_metadata(env, 'client')
    ddb.delete_item(
        TableName='Variables',
        Key={'id': {'S': variable_id}}
    )


def scan_variables(env):
    variables_raw = scan_dynamodb_table(env, 'Variables')
    return [Variable(**var).__dict__ for var in variables_raw]
