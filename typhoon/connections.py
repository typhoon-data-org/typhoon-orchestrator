import os
from typing import NamedTuple, Optional

import yaml
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from typhoon.aws import connect_dynamodb_metadata
from typhoon.settings import get_env, typhoon_directory


class ConnectionParams(NamedTuple):
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[dict] = None


class Connection(NamedTuple):
    conn_id: str
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[dict] = None


def get_connection_params(conn_id: str) -> ConnectionParams:
    conn = get_connection(get_env(), conn_id)
    return ConnectionParams(**conn._asdict().pop('conn_id'))


def get_connection_local(conn_id: str, conn_env: Optional[str]) -> ConnectionParams:
    connections_yml = os.path.join(typhoon_directory(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f)
    conn_params = connections[conn_id] if not conn_env else connections[conn_id][conn_env]
    return ConnectionParams(**conn_params)


def set_connection(env: str, conn_id: str, conn_params: ConnectionParams):
    ddb = connect_dynamodb_metadata(env, 'client')
    serializer = TypeSerializer()
    ddb.put_item(
        TableName='Connections',
        Item={
            'conn_id': {'S': conn_id},
            **serializer.serialize(conn_params._asdict())['M']
        })


def get_connection(env: str, conn_id: str) -> Connection:
    ddb = connect_dynamodb_metadata(env, 'client')
    response = ddb.get_item(
        TableName='Connections',
        Key={'conn_id': {'S': conn_id}}
    )
    if 'Item' not in response:
        raise ValueError(f'Connection {conn_id} is not defined')
    deserializer = TypeDeserializer()
    conn = {k: deserializer.deserialize(v) for k, v in response['Item'].items()}
    return Connection(**conn)


def delete_connection(env: str, conn_id: str):
    ddb = connect_dynamodb_metadata(env, 'client')
    ddb.delete_item(
        TableName='Connections',
        Key={'conn_id': {'S': conn_id}}
    )
