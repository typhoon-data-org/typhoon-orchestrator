import json
import os
from decimal import Decimal
from typing import NamedTuple, Optional

import yaml
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from dataclasses import dataclass

from typhoon.aws import connect_dynamodb_metadata, scan_dynamodb_table, replace_decimals
from typhoon.settings import get_env, typhoon_directory


@dataclass
class ConnectionParams:
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[dict] = None

    def __post_init__(self):
        self.port = replace_decimals(self.port)
        self.extra = replace_decimals(self.extra)


@dataclass
class Connection:
    conn_id: str
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[dict] = None

    def __post_init__(self):
        self.port = replace_decimals(self.port)
        self.extra = replace_decimals(self.extra)


def get_connection_params(conn_id: str) -> ConnectionParams:
    conn = get_connection(get_env(), conn_id)
    conn_params = conn.__dict__
    conn_params.pop('conn_id')
    return ConnectionParams(**conn_params)


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
            **serializer.serialize(conn_params.__dict__)['M']
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


def dump_connections(env: str, dump_format='json') -> str:
    """Prints the connections in json/yaml format"""
    connections_raw = scan_dynamodb_table(env, 'Connections')
    connections = {}
    for conn in connections_raw:
        k = conn.pop('conn_id')
        v = ConnectionParams(**conn).__dict__
        connections[k] = v

    if dump_format == 'json':
        return json.dumps(connections, indent=4)
    elif dump_format == 'yaml':
        represent_dict_order = lambda self, data: self.represent_mapping('tag:yaml.org,2002:map', data.items())
        yaml.add_representer(dict, represent_dict_order)
        return yaml.dump(connections, default_flow_style=False)
    else:
        ValueError(f'Format {dump_format} is not supported. Choose json/yaml')
