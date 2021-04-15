from pathlib import Path
from typing import List

import yaml

from typhoon.core.glue import load_dags, load_dag
from typhoon.core.settings import Settings
from typhoon.remotes import Remotes
from typhoon.variables import VariableType


PROJECT_TEMPLATES = list(x.name for x in (Path(__file__).parent.parent / 'examples').iterdir())


def get_project_templates(ctx, args, incomplete) -> List[str]:
    return [x for x in PROJECT_TEMPLATES if incomplete in x]


def get_deploy_targets(ctx, args, incomplete) -> List[str]:
    return [x for x in ['airflow', 'typhoon'] if incomplete in x]


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]


def get_dag_names(ctx, args, incomplete) -> List[str]:
    return [dag.name for dag, _ in load_dags(ignore_errors=True) if incomplete in dag.name]


def get_node_names(ctx, args, incomplete) -> List[str]:
    dag_name_index = args.index('--dag-name') + 1
    dag_name = args[dag_name_index]
    result = load_dag(dag_name, ignore_errors=True)
    if result is None:
        return []
    dag, _ = result
    return [x for x in dag.nodes.keys() if incomplete in x]


def get_edge_names(ctx, args, incomplete) -> List[str]:
    dag_name_index = args.index('--dag-name') + 1
    dag_name = args[dag_name_index]
    result = load_dag(dag_name, ignore_errors=True)
    if result is None:
        return []
    dag, _ = result
    return [x for x in dag.edges.keys() if incomplete in x]


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
