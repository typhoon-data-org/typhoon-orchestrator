import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import pkg_resources
from dataclasses import asdict
from tabulate import tabulate
from termcolor import colored

from typhoon import local_config, connections
from typhoon.cli_helpers.cli_completion import get_remote_names, get_dag_names, get_conn_envs, get_conn_ids, \
    get_var_types
from typhoon.cli_helpers.status import dags_with_changes, dags_without_deploy, check_connections_yaml, \
    check_connections_dags, check_variables_dags
from typhoon.connections import Connection
from typhoon.core.glue import get_dag_errors
from typhoon.core.settings import Settings, EnvVarName
from typhoon.deployment.packaging import build_all_dags
from typhoon.handler import run_dag
from typhoon.local_config import EXAMPLE_CONFIG
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
from typhoon.remotes import Remotes
from typhoon.variables import Variable, VariableType
from typhoon.watch import watch_changes

ascii_art_logo = r"""
 _________  __  __   ______   ___   ___   ______   ______   ___   __        
/________/\/_/\/_/\ /_____/\ /__/\ /__/\ /_____/\ /_____/\ /__/\ /__/\      
\__.::.__\/\ \ \ \ \\:::_ \ \\::\ \\  \ \\:::_ \ \\:::_ \ \\::\_\\  \ \     
   \::\ \   \:\_\ \ \\:(_) \ \\::\/_\ .\ \\:\ \ \ \\:\ \ \ \\:. `-\  \ \    
    \::\ \   \::::_\/ \: ___\/ \:: ___::\ \\:\ \ \ \\:\ \ \ \\:. _    \ \   
     \::\ \    \::\ \  \ \ \    \: \ \\::\ \\:\_\ \ \\:\_\ \ \\. \`-\  \ \  
      \__\/     \__\/   \_\/     \__\/ \::\/ \_____\/ \_____\/ \__\/ \__\/  
"""


@click.group()
def cli():
    """Typhoon CLI"""
    home = local_config.find_typhoon_home_in_cwd_or_parents()
    if not home:
        print('Did not find typhoon in current directory or any of its parent directories')
        if Settings.typhoon_home:
            print(f'${EnvVarName.PROJECT_HOME} defined to "{Settings.typhoon_home}"')
        else:
            return
    else:
        Settings.typhoon_home = home
    try:
        Settings.project_name = local_config.project_name()
    except KeyError:
        print(f'Project name not set in "{Settings.typhoon_home}/typhoon.cfg "')


@cli.command()
@click.argument('project_name')
def init(project_name: str):
    """Create a new Typhoon project"""
    example_project_path = Path(pkg_resources.resource_filename('typhoon', 'examples')) / 'hello_world'
    dest = Path.cwd() / project_name
    shutil.copytree(str(example_project_path), str(dest))
    (dest / 'typhoon.cfg').write_text(EXAMPLE_CONFIG.format(project_name=project_name))
    print(f'Project created in {dest}')


@cli.command()
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def status(remote: Optional[str]):
    """Information on project status"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote

    print(colored(ascii_art_logo, 'cyan'))
    if not Settings.typhoon_home:
        print(colored(f'FATAL: typhoon home not found...', 'red'))
        return
    else:
        print(colored('• Typhoon home defined as', 'green'), colored(Settings.typhoon_home, 'grey'))

    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if metadata_store.exists():
        print(colored('• Metadata database found in', 'green'), colored(Settings.metadata_db_url, 'grey'))
        check_connections_yaml(remote)
        check_connections_dags(remote)
        check_variables_dags(remote)
    elif isinstance(metadata_store, SQLiteMetadataStore):
        print(colored('• Metadata store not found for', 'yellow'), colored(Settings.metadata_db_url, 'grey'))
        print(
            colored('   - It will be created upon use, or create by running (idempotent) command', color='blue'),
            colored(f'typhoon migrate{" " + remote if remote else ""}', 'grey'))
        print(colored('  Skipping connections and variables checks...', 'red'))
    else:
        print(colored('• Metadata store not found or incomplete for', 'red'), colored(Settings.metadata_db_url, 'grey'))
        print(
            colored('   - Fix by running (idempotent) command', color='blue'),
            colored(f'typhoon migrate{" " + remote if remote else ""}', 'grey'))
        print(colored('  Skipping connections and variables checks...', 'red'))

    if not remote:
        changed_dags = dags_with_changes()
        if changed_dags:
            print(colored('• Unbuilt changes in DAGs...', 'yellow'), colored('To rebuild run', 'white'),
                  colored(f'typhoon build-dags {remote} [--debug]', 'grey'))
            for dag in changed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))
    else:
        undeployed_dags = dags_without_deploy(remote)
        if undeployed_dags:
            print(colored('• Undeployed changes in DAGs...', 'yellow'), colored('To deploy run', 'white'),
                  colored(f'typhoon deploy-dags {remote} [--build-dependencies]', 'grey'))
            for dag in undeployed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))


@cli.group(name='remote')
def cli_remote():
    """Manage Typhoon remotes"""
    pass


@cli_remote.command(name='add')
@click.argument('remote')       # No autocomplete because the remote is new
@click.option('--aws-profile')
@click.option('--metadata-db-url')
@click.option('--use-name-as-suffix', is_flag=True, default=False)
def remote_add(remote: str, aws_profile: str, metadata_db_url: str, use_name_as_suffix: bool):
    """Add a remote for deployments and management"""
    Remotes.add_remote(remote, aws_profile, metadata_db_url, use_name_as_suffix)
    print(f'Added remote {remote}')


@cli_remote.command(name='ls')
@click.option('-l', '--long', is_flag=True, default=False)
def remote_list(long: bool):
    """List configured Typhoon remotes"""
    if long:
        header = ['REMOTE_NAME', 'AWS_PROFILE', 'USE_NAME_AS_SUFFIX', 'METADATA_DB_URL']
        table_body = [
            [remote, Remotes.aws_profile(remote), Remotes.use_name_as_suffix(remote), Remotes.metadata_db_url(remote)]
            for remote in Remotes.remote_names
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for remote in Remotes.remote_names:
            print(remote)


@cli_remote.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names)       # No autocomplete because the remote is new
def remote_add(remote: str):
    """Remove remote"""
    Remotes.remove_remote(remote)
    print(f'Removed remote {remote}')


@cli.group(name='metadata')
def cli_metadata():
    """Manage Typhoon metadata"""
    pass


@cli_metadata.command()
@click.argument('remote', autocompletion=get_remote_names)
def migrate(remote: str):
    """Create the necessary metadata tables"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    print(f'Migrating {Settings.metadata_db_url}...')
    Settings.metadata_store(aws_profile=Remotes.aws_profile(remote)).migrate()


@cli_metadata.command(name='info')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def metadata_info(remote: Optional[str]):
    """Info on metadata connection and table names"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    print(ascii_art_logo)
    print(f'Metadata database URL:\t{Settings.metadata_db_url}')
    print(f'Connections table name:\t{Settings.connections_table_name}')
    print(f'Variables table name:\t{Settings.variables_table_name}')
    print(f'DAG deployments table name:\t{Settings.dag_deployments_table_name}')


@cli.group(name='dag')
def cli_dags():
    """Manage Typhoon DAGs"""
    pass


@cli_dags.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_dags(remote: Optional[str], long: bool):
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        header = ['DAG_NAME', 'DEPLOYMENT_DATE']
        table_body = [
            [x.dag_name, x.deployment_date.isoformat()]
            for x in metadata_store.get_dag_deployments()
        ]
        print(tabulate(table_body, header, 'plain'))
        if not remote:
            dag_errors = get_dag_errors()
            if dag_errors:
                header = ['DAG_NAME', 'ERROR LOCATION', 'ERROR MESSAGE']
                table_body = [
                    [dag_name, error[0]['loc'], error[0]['msg']]
                    for dag_name, error in dag_errors.items()
                ]
                print(colored(tabulate(table_body, header, 'plain'), 'red'), file=sys.stderr)
    else:
        for dag_deployment in metadata_store.get_dag_deployments():
            print(dag_deployment.dag_name)
        if not remote:
            for dag_name, _ in get_dag_errors().items():
                print(colored(dag_name, 'red'), file=sys.stderr)


@cli_dags.command(name='build')
@click.argument('dag_name', autocompletion=get_dag_names, required=False, default=None)
@click.option('--all', 'all_', is_flag=True, default=False, help='Build all DAGs (mutually exclusive with DAG_NAME)')
def build_dags(dag_name: Optional[str], all_: bool):
    """Build code for dags in $TYPHOON_HOME/out/"""
    if dag_name and all_:
        raise click.UsageError(f'Illegal usage: DAG_NAME is mutually exclusive with --all')
    elif dag_name is None and not all_:
        raise click.UsageError(f'Illegal usage: Need either DAG_NAME or --all')
    if all_:
        build_all_dags(remote=None)
    else:
        dag_errors = get_dag_errors().get(dag_name)
        if dag_errors:
            print(f'FATAL: DAG {dag_name} has errors', file=sys.stderr)
            header = ['ERROR_LOCATION', 'ERROR_MESSAGE']
            table_body = [
                [error['loc'], error['msg']]
                for error in dag_errors
            ]
            print(tabulate(table_body, header, 'plain'), file=sys.stderr)
            sys.exit(-1)
        build_all_dags(remote=None, matching=dag_name)


@cli_dags.command(name='watch')
@click.argument('dag_name', autocompletion=get_dag_names, required=False, default=None)
@click.option('--all', 'all_', is_flag=True, default=False, help='Build all DAGs (mutually exclusive with DAG_NAME)')
def watch_dags(dag_name: Optional[str], all_: bool):
    """Watch DAGs and build code for dags in $TYPHOON_HOME/out/"""
    if dag_name and all_:
        raise click.UsageError(f'Illegal usage: DAG_NAME is mutually exclusive with --all')
    elif dag_name is None and not all_:
        raise click.UsageError(f'Illegal usage: Need either DAG_NAME or --all')
    if all_:
        print('Watching all DAGs for changes...')
        watch_changes()
    else:
        print(f'Watching DAG {dag_name} for changes...')
        watch_changes(patterns=f'{dag_name}.yml')


def run_local_dag(dag_name: str, execution_date: datetime):
    dag_path = Settings.out_directory / dag_name / f'{dag_name}.py'
    if not dag_path.exists():
        print(f"Error: {dag_path} doesn't exist. Build DAGs")
    try:
        run_dag(dag_name, str(execution_date), capture_logs=False)
    except FileNotFoundError:
        print(f'DAG {dag_name} could not be built')


@cli_dags.command(name='run')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--dag-name', autocompletion=get_dag_names)
@click.option('--execution-date', default=None, is_flag=True, type=click.DateTime(), help='DAG execution date as YYYY-mm-dd')
def cli_run_dag(remote: Optional[str], dag_name: str, execution_date: Optional[datetime]):
    """Run a DAG for a specific date. Will create a metadata entry in the database (TODO: create entry)."""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    if execution_date is None:
        execution_date = datetime.now()
    if remote is None:
        print(f'Running {dag_name} from local build...')
        dag_errors = get_dag_errors().get(dag_name)
        if dag_errors:
            print(f'FATAL: DAG {dag_name} has errors', file=sys.stderr)
            header = ['ERROR_LOCATION', 'ERROR_MESSAGE']
            table_body = [
                [error['loc'], error['msg']]
                for error in dag_errors
            ]
            print(tabulate(table_body, header, 'plain'), file=sys.stderr)
            sys.exit(-1)

        build_all_dags(remote=None, matching=dag_name)
        # Sets the env variable for metadata store to the sqlite in CWD if not set, because the CWD will be different at
        # runtime
        Settings.metadata_db_url = Settings.metadata_db_url
        run_local_dag(dag_name, execution_date)
    else:
        # TODO: Run lambda function
        pass


@cli.group(name='connection')
def cli_connection():
    """Manage Typhoon connections"""
    pass


@cli_connection.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_connections(remote: Optional[str], long: bool):
    """List connections in the metadata store"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        header = ['CONN_ID', 'TYPE', 'HOST', 'PORT', 'SCHEMA']
        table_body = [
            [conn.conn_id, conn.conn_type, conn.host, conn.port, conn.schema]
            for conn in metadata_store.get_connections()
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for conn in metadata_store.get_connections():
            print(conn.conn_id)


@cli_connection.command(name='add')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--conn-id', autocompletion=get_conn_ids)
@click.option('--conn-env', autocompletion=get_conn_envs)
def add_connection(remote: Optional[str], conn_id: str, conn_env: str):
    """Add connection to the metadata store"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    conn_params = connections.get_connection_local(conn_id, conn_env)
    metadata_store.set_connection(Connection(conn_id=conn_id, **asdict(conn_params)))
    print(f'Connection {conn_id} added')


@cli_connection.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--conn-id', autocompletion=get_conn_ids)
def remove_connection(remote: Optional[str], conn_id: str):
    """Remove connection from the metadata store"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    metadata_store.delete_connection(conn_id)
    print(f'Connection {conn_id} deleted')


@cli.group(name='variable')
def cli_variable():
    """Manage Typhoon variables"""
    pass


@cli_variable.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_variables(remote: Optional[str], long: bool):
    """List variables in the metadata store"""
    def var_contents(var: Variable) -> str:
        if var.type == VariableType.NUMBER:
            return var.contents
        else:
            return f'"{var.contents}"' if len(var.contents) < max_len_var else f'"{var.contents[:max_len_var]}"...'
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        max_len_var = 40
        header = ['VAR_ID', 'TYPE', 'CONTENT']
        table_body = [
            [var.id, var.type, var_contents(var)]
            for var in metadata_store.get_variables()
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for var in metadata_store.get_variables():
            print(var.id)


@cli_variable.command(name='add')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--var-id')
@click.option('--var-type', autocompletion=get_var_types)
@click.option('--contents')
def add_variable(remote: Optional[str], var_id: str, var_type: str, contents):
    """Add variable to the metadata store"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    var = Variable(var_id, VariableType[var_type.upper()], contents)
    metadata_store.set_variable(var)
    print(f'Variable {var_id} added')


@cli_variable.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--var-id')
def remove_variable(remote: Optional[str], var_id: str):
    """Remove connection from the metadata store"""
    if remote:
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    metadata_store.delete_variable(var_id)
    print(f'Variable {var_id} deleted')


class SubprocessError(Exception):
    pass


def run_in_subprocess(command: str, cwd: str):
    print(f'Executing command in shell:  {command}')
    args = command.split(' ')
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=os.environ.copy(), cwd=cwd)
    stdout, stderr = p.communicate()
    stdout = stdout.decode()
    if stderr:
        raise SubprocessError(f'Error executing in console: {stderr}')
    elif 'Unable to upload artifact' in stdout:
        print(stdout)
        raise SubprocessError(f'Error executing in console: {stdout}')
    print(stdout)


# @cli.command()
# @click.argument('remote', autocompletion=get_remote_names)
# @click.option('--build-dependencies', default=False, is_flag=True, help='Build DAG dependencies in Docker container')
# def deploy_dags(remote, build_dependencies):
#     from typhoon.core import get_typhoon_config
#
#     config = get_typhoon_config(use_cli_config=True, target_env=target_env)
#
#     build_all_dags(target_env=target_env, debug=False)
#     if build_dependencies:
#         run_in_subprocess(f'sam build --use-container', cwd=out_directory())
#     build_dir = str(Path(out_directory()) / '.aws-sam' / 'build')
#     run_in_subprocess(
#         f'sam package --template-file template.yaml --s3-bucket {config.s3_bucket} --profile {config.aws_profile}'
#         f' --output-template-file out_template.yaml',
#         cwd=build_dir
#     )
#     run_in_subprocess(
#         f'sam deploy --template-file out_template.yaml '
#         f'--stack-name {config.project_name.replace("_", "-")} --profile {config.aws_profile} '
#         f'--region {config.deploy_region} --capabilities CAPABILITY_IAM',
#         cwd=build_dir
#     )
#
#     if not config.development_mode:
#         for dag_code in get_dags_contents(settings.dags_directory()):
#             loaded_dag = yaml.safe_load(dag_code)
#             if loaded_dag.get('active', True):
#                 dag_deployment = DagDeployment(loaded_dag['name'], deployment_date=datetime.utcnow(), dag_code=dag_code)
#                 config.metadata_store.set_dag_deployment(dag_deployment)

if __name__ == '__main__':
    cli()
