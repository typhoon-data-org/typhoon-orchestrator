from typing import Optional

import yaml
from dataclasses import dataclass

from typhoon.aws.dynamodb_helper import replace_decimals
from typhoon.core.settings import Settings


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

    def to_conn(self, conn_id: str) -> 'Connection':
        return Connection(conn_id=conn_id, **self.__dict__)


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
        if not isinstance(self.password, str) and self.password is not None:
            self.password = str(self.password)

    def get_connection_params(self) -> ConnectionParams:
        conn_params = self.__dict__.copy()
        conn_params.pop('conn_id')
        return ConnectionParams(**conn_params)


def get_connection_local(conn_id: str, conn_env: Optional[str]) -> ConnectionParams:
    connections_yml = Settings.typhoon_home / 'connections.yml'
    connections = yaml.safe_load(connections_yml.read_text())
    conn_params = connections[conn_id] if not conn_env else connections[conn_id][conn_env]
    return ConnectionParams(**conn_params)


def get_connections_local_by_conn_id(conn_id: str) -> dict:
    connections_yml = Settings.typhoon_home / 'connections.yml'
    connections = yaml.safe_load(connections_yml.read_text())
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
