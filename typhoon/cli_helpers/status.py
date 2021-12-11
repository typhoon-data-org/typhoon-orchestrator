import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import yaml
from termcolor import colored

from typhoon.core.dags import DagDeployment, add_yaml_constructors
from typhoon.core.glue import get_dag_filenames, get_dags_contents
from typhoon.core.metadata_store_interface import MetadataObjectNotFound
from typhoon.core.settings import Settings
from typhoon.remotes import Remotes


def dags_with_changes() -> List[str]:
    add_yaml_constructors()
    result = []
    for dag_file in get_dag_filenames():
        yaml_path = Path(Settings.dags_directory) / dag_file
        yaml_modified_ts = datetime.fromtimestamp(yaml_path.stat().st_mtime)
        dag_name = yaml.load(yaml_path.read_text(), yaml.FullLoader)['name']
        transpiled_path = Path(Settings.out_directory) / dag_name / f'{dag_name}.py'
        if not transpiled_path.exists():
            continue
        transpiled_created_ts = datetime.fromtimestamp(transpiled_path.stat().st_ctime)
        if yaml_modified_ts > transpiled_created_ts:
            result.append(dag_name)

    return result


def dags_without_deploy(remote: Optional[str]) -> List[str]:
    add_yaml_constructors()
    undeployed_dags = []
    for dag_code in get_dags_contents(Settings.dags_directory):
        loaded_dag = yaml.load(dag_code, yaml.FullLoader)
        dag_deployment = DagDeployment(dag_name=loaded_dag['name'], deployment_date=datetime.utcnow(), dag_code=dag_code)
        metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
        if loaded_dag.get('active', True):
            try:
                _ = metadata_store.get_dag_deployment(dag_deployment.deployment_hash)
            except MetadataObjectNotFound:
                undeployed_dags.append(dag_deployment.dag_name)
    return undeployed_dags


def get_undefined_connections_in_metadata_db(remote: Optional[str], conn_ids: List[str]):
    undefined_connections = []
    for conn_id in conn_ids:
        try:
            Settings.metadata_store(Remotes.aws_profile(remote)).get_connection(conn_id)
        except MetadataObjectNotFound:
            undefined_connections.append(conn_id)
    return undefined_connections


def get_undefined_variables_in_metadata_db(remote: Optional[str], var_ids: List[str]):
    undefined_variables = []
    for var_id in var_ids:
        try:
            Settings.metadata_store(Remotes.aws_profile(remote)).get_variable(var_id)
        except MetadataObjectNotFound:
            undefined_variables.append(var_id)
    return undefined_variables


def check_connections_yaml(remote: Optional[str]):
    connections_yaml_path = Settings.typhoon_home / 'connections.yml'
    if not connections_yaml_path.exists():
        print(colored('• Connections YAML not found. To add connections create', 'red'), colored('connections.yml', 'blue'))
        print(colored('  Skipping connections YAML checks...', 'red'))
        return
    conn_yml = yaml.safe_load(connections_yaml_path.read_text()) or {}
    undefined_connections = get_undefined_connections_in_metadata_db(remote, conn_ids=conn_yml.keys())
    if undefined_connections:
        print(colored('• Found connections in YAML that are not defined in the metadata database', 'yellow'))
        for conn_id in undefined_connections:
            print(
                colored('   - Connection', 'yellow'),
                colored(conn_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon connection add{" " + remote if remote else ""} --conn-id {conn_id} --conn-env CONN_ENV', 'blue')
            )
    else:
        print(colored('• All connections in YAML are defined in the database', 'green'))


def check_connections_dags(remote: Optional[str]):
    all_conn_ids = set()
    for dag_file in Path(Settings.dags_directory).rglob('*.yml'):
        conn_ids = re.findall(r'(?:\$HOOK\.|!Hook\s+)(\w+)', dag_file.read_text())
        all_conn_ids = all_conn_ids.union(conn_ids)
    undefined_connections = get_undefined_connections_in_metadata_db(remote, conn_ids=all_conn_ids)
    if undefined_connections:
        print(colored('• Found connections in DAGs that are not defined in the metadata database', 'yellow'))
        for conn_id in undefined_connections:
            print(
                colored('   - Connection', 'yellow'),
                colored(conn_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon connection add{" " + remote if remote else ""} --conn-id {conn_id} --conn-env CONN_ENV', 'blue')
            )
    else:
        print(colored('• All connections in the DAGs are defined in the database', 'green'))


def check_variables_dags(remote: Optional[str]):
    all_var_ids = set()
    for dag_file in Path(Settings.dags_directory).rglob('*.yml'):
        var_ids = re.findall(r'(?:\$VARIABLE\.|!Var\s+)(\w+)', dag_file.read_text())
        all_var_ids = all_var_ids.union(var_ids)
    undefined_variables = get_undefined_variables_in_metadata_db(remote, var_ids=all_var_ids)
    if undefined_variables:
        print(colored('• Found variables in DAGs that are not defined in the metadata database', 'yellow'))
        for var_id in undefined_variables:
            print(
                colored('   - Variable', 'yellow'),
                colored(var_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon variable add {" " + remote if remote else ""} --var-id {var_id} --var-type VAR_TYPE --contents VALUE', 'blue')
            )
    else:
        print(colored('• All variables in the DAGs are defined in the database', 'green'))