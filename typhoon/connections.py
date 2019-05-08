import json
import os
from typing import Optional, Iterable

import yaml
from dataclasses import dataclass

from typhoon.aws.plumbing import dynamodb_plumbing
from typhoon.aws.plumbing.dynamodb_plumbing import replace_decimals
from typhoon.core import get_typhoon_config
from typhoon.settings import typhoon_directory


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

    def get_connection_params(self) -> ConnectionParams:
        conn_params = self.__dict__.copy()
        conn_params.pop('conn_id')
        return ConnectionParams(**conn_params)


def get_connection_local(conn_id: str, conn_env: Optional[str]) -> ConnectionParams:
    connections_yml = os.path.join(typhoon_directory(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f, Loader=yaml.FullLoader)
    conn_params = connections[conn_id] if not conn_env else connections[conn_id][conn_env]
    return ConnectionParams(**conn_params)


def get_connections_local_by_conn_id(conn_id: str) -> dict:
    connections_yml = os.path.join(typhoon_directory(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f, Loader=yaml.FullLoader)
    connections = connections[conn_id]
    return connections


def set_connection(
        conn_id: str,
        conn_params: ConnectionParams,
        use_cli_config: bool = False,
        target_env: Optional[str] = None,
):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.dynamodb_put_item(
        ddb_client=config.dynamodb_client,
        table_name=config.connections_table_name,
        item={
            'conn_id': conn_id,
            **conn_params.__dict__,
        }
    )


def get_connection(
        conn_id: str,
        use_cli_config: bool = False,
        target_env: Optional[str] = None,
) -> Connection:
    # ddb_client = dynamodb_client_from_config(config)
    # response = ddb_client.get_item(
    #     TableName='Connections',
    #     Key={'conn_id': {'S': conn_id}}
    # )
    # if 'Item' not in response:
    #     raise ValueError(f'Connection {conn_id} is not defined')
    # deserializer = TypeDeserializer()
    # conn = {k: deserializer.deserialize(v) for k, v in response['Item'].items()}
    # return Connection(**conn)
    config = get_typhoon_config(use_cli_config, target_env)
    item = dynamodb_plumbing.dynamodb_get_item(
        ddb_client=config.dynamodb_client,
        table_name=config.connections_table_name,
        key_name='conn_id',
        key_value=conn_id,
    )
    return Connection(**item)


def get_connection_params(
        conn_id: str,
        use_cli_config: bool = False,
        target_env: Optional[str] = None,
) -> ConnectionParams:
    conn = get_connection(conn_id, use_cli_config, target_env)
    return conn.get_connection_params()


def delete_connection(conn_id: str, use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.dynamodb_delete_item(
        ddb_client=config.dynamodb_client,
        table_name=config.connections_table_name,
        key_name='conn_id',
        key_value=conn_id,
    )


def scan_connections(to_dict: bool = False, use_cli_config: bool = False, target_env: Optional[str] = None) -> Iterable[Connection]:
    config = get_typhoon_config(use_cli_config, target_env)
    connections_raw = dynamodb_plumbing.scan_dynamodb_table(
        ddb_resource=config.dynamodb_resource,
        table_name=config.connections_table_name,
    )
    return [Connection(**conn).__dict__ if to_dict else Connection(**conn) for conn in connections_raw]


def scan_connection_params(
        use_cli_config: bool = False,
        target_env: Optional[str] = None
) -> Iterable[ConnectionParams]:
    connections = scan_connections(use_cli_config, target_env)
    return [conn.get_connection_params() for conn in connections]


def dump_connections(use_cli_config: bool = False, target_env: Optional[str] = None, dump_format='json') -> str:
    """Prints the connections in json/yaml format"""
    connections = scan_connection_params(use_cli_config, target_env)

    if dump_format == 'json':
        return json.dumps(connections, indent=4)
    elif dump_format == 'yaml':
        def _represent_dict_order(self, data):
            return self.represent_mapping('tag:yaml.org,2002:map', data.items())
        yaml.add_representer(dict, _represent_dict_order)
        return yaml.dump(connections, default_flow_style=False)
    else:
        ValueError(f'Format {dump_format} is not supported. Choose json/yaml')
