import os
from typing import NamedTuple, Optional

import yaml

from typhoon.settings import out_directory, get_env


class ConnectionParams(NamedTuple):
    conn_type: str
    host: Optional[str]
    port: Optional[int]
    login: Optional[str]
    password: Optional[str]
    schema: Optional[str]
    extra: Optional[dict]


def get_connection(conn_id: str) -> ConnectionParams:
    env = get_env()
    if env == 'dev':
        return get_connection_local(conn_id)
    else:
        raise ValueError(f'Environment f{env} not recognized')


def get_connection_local(conn_id: str) -> ConnectionParams:
    connections_yml = os.path.join(out_directory(), 'connections.yml')
    with open(connections_yml, 'r') as f:
        connections = yaml.load(f)
    return ConnectionParams(**connections[conn_id])
