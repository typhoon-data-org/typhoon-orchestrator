from typing import List

import yaml

from typhoon.core.glue import load_dags
from typhoon.core.settings import Settings
from typhoon.remotes import Remotes
from typhoon.variables import VariableType


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]


def get_dag_names(ctx, args, incomplete) -> List[str]:
    return [dag.name for dag, _ in load_dags()]


def get_conn_ids(ctx, args, incomplete) -> List[str]:
    connections_yml = Settings.typhoon_home / 'connections.yml'
    connections = yaml.safe_load(connections_yml.read_text())
    return [x for x in connections.keys() if incomplete in x]


def get_conn_envs(ctx, args, incomplete) -> List[str]:
    connections_yml = Settings.typhoon_home / 'connections.yml'
    connections = yaml.safe_load(connections_yml.read_text())
    conn_id_index = args.index('--conn-id') + 1
    conn_id = args[conn_id_index]
    return [x for x in connections.get(conn_id, {}).keys() if incomplete in x]


def get_var_types(ctx, args, incomplete) -> List[str]:
    return [x.lower() for x in VariableType if incomplete in x.lower()]
