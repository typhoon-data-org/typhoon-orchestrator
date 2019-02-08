import os
from typing import NamedTuple, Optional

import yaml
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from typhoon.aws import connect_dynamodb_metadata, dict_to_ddb_item
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
    env = get_env()
    if env == 'dev':
        return get_connection_local(conn_id)
    else:
        raise ValueError(f'Environment f{env} not recognized')


def get_connection_local(conn_id: str) -> ConnectionParams:
    connections_yml = os.path.join(os.path.dirname(typhoon_directory()), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f)
    return ConnectionParams(**connections[conn_id])


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
