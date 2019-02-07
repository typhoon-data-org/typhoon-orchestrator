import os
from typing import NamedTuple, Optional

import yaml

from typhoon.settings import get_env, typhoon_directory


class ConnectionParams(NamedTuple):
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
