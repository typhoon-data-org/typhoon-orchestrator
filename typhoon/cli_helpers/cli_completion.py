from typing import List

import yaml

from typhoon.core.settings import Settings
from typhoon.remotes import Remotes


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]


def get_dag_names(ctx, args, incomplete) -> List[str]:
    return [x.dag_name for x in Settings.metadata_store(aws_profile=None).get_dag_deployments() if incomplete in x.dag_name]


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
