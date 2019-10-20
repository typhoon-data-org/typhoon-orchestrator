import os
from typing import Optional

import yaml
from dataclasses import dataclass

from typhoon.aws.plumbing.dynamodb_plumbing import replace_decimals
from typhoon.core.settings import typhoon_home


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
    connections_yml = os.path.join(typhoon_home(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f, Loader=yaml.FullLoader)
    conn_params = connections[conn_id] if not conn_env else connections[conn_id][conn_env]
    return ConnectionParams(**conn_params)


def get_connections_local_by_conn_id(conn_id: str) -> dict:
    connections_yml = os.path.join(typhoon_home(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f, Loader=yaml.FullLoader)
    connections = connections[conn_id]
    return connections


# def dump_connections(use_cli_config: bool = False, target_env: Optional[str] = None, dump_format='json') -> str:
#     """Prints the connections in json/yaml format"""
#     connections = scan_connection_params(use_cli_config, target_env)
#
#     if dump_format == 'json':
#         return json.dumps(connections, indent=4)
#     elif dump_format == 'yaml':
#         def _represent_dict_order(self, data):
#             return self.represent_mapping('tag:yaml.org,2002:map', data.items())
#         yaml.add_representer(dict, _represent_dict_order)
#         return yaml.dump(connections, default_flow_style=False)
#     else:
#         ValueError(f'Format {dump_format} is not supported. Choose json/yaml')
