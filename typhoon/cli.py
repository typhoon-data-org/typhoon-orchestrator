import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click
import pkg_resources
from termcolor import colored

from typhoon import local_config
from typhoon.cli_helpers.cli_completion import get_remote_names
from typhoon.cli_helpers.status import dags_with_changes, dags_without_deploy, check_connections_yaml, \
    check_connections_dags, check_variables_dags
from typhoon.core.config import CLIConfig
from typhoon.core.glue import transpile_dag_and_store, load_dags
from typhoon.core.settings import Settings, EnvVarName
from typhoon.deployment.deploy import deploy_dag_requirements, copy_local_typhoon, copy_user_defined_code
from typhoon.local_config import EXAMPLE_CONFIG
from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
from typhoon.remotes import Remotes

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
@click.argument('project_name', default='hello_world')
def init(project_name: str):
    example_project_path = Path(pkg_resources.resource_filename('typhoon', 'examples')) / 'hello_world'
    dest = Path.cwd() / project_name
    shutil.copytree(str(example_project_path), str(dest))
    (dest / 'typhoon.cfg').write_text(EXAMPLE_CONFIG.format(project_name=project_name))
    print(f'Project created in {dest}')


@cli.command()
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def status(remote: Optional[str]):
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
        print('REMOTE_NAME\tAWS_PROFILE\tUSE_NAME_AS_SUFFIX\tMETADATA_DB_URL')
    for remote in Remotes.remote_names:
        if long:
            print(f'{remote}\t{Remotes.aws_profile(remote)}\t{Remotes.use_name_as_suffix(remote)}\t{Remotes.metadata_db_url(remote)}')
        else:
            print(remote)


@cli_remote.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names)       # No autocomplete because the remote is new
def remote_add(remote: str):
    """Remove remote"""
    Remotes.remove_remote(remote)
    print(f'Removed remote {remote}')


@cli.command()
@click.argument('remote', autocompletion=get_remote_names)
def migrate(remote: str):
    """Create the necessary metadata tables"""
    Settings.metadata_store(aws_profile=Remotes.aws_profile(remote)).migrate()


@cli.command()
def build_dags():
    """Build code for dags in $TYPHOON_HOME/out/"""
    print(ascii_art_logo)
    build_all_dags(remote=None)


def dist_is_editable(dist) -> bool:
    """Is distribution an editable install?"""
    for path_item in sys.path:
        egg_link = os.path.join(path_item, dist.project_name + '.egg-link')
        if os.path.isfile(egg_link):
            return True
    return False


def typhoon_version_is_local() -> bool:
    typhoon_dist = [dist for dist in pkg_resources.working_set if dist.key == 'typhoon-orchestrator'][0]
    return dist_is_editable(typhoon_dist)


def local_typhoon_path() -> str:
    import typhoon
    import inspect
    return str(Path(inspect.getfile(typhoon)).parent)


def build_all_dags(remote: Optional[str]):
    from typhoon.deployment.deploy import clean_out
    from typhoon.deployment.sam import deploy_sam_template

    clean_out()

    print('Build all DAGs...')
    dags = load_dags()
    deploy_sam_template(dags, remote=remote)
    for dag in dags:
        dag = dag.dict()
        dag_folder = Settings.out_directory / dag['name']
        transpile_dag_and_store(dag, dag_folder / f"{dag['name']}.py", remote=remote)

        deploy_dag_requirements(dag, typhoon_version_is_local(), Settings.typhoon_version)
        if typhoon_version_is_local():
            print('Typhoon package is in editable mode. Copying to lambda package...')
            copy_local_typhoon(dag, local_typhoon_path())
        if not remote:
            print('Setting up user defined code as symlink for debugging...')
        copy_user_defined_code(dag, symlink=remote is None)

    print('Finished building DAGs\n')


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
#
#
# @cli.command()
# @click.argument('conn_id')
# @click.argument('conn_env')
# @click.argument('target_env', autocompletion=get_environments)
# def set_connection(conn_id, conn_env, target_env):
#     conn_params = connections.get_connection_local(conn_id, conn_env)
#     config = CLIConfig(target_env)
#     config.metadata_store.set_connection(Connection(conn_id=conn_id, **asdict(conn_params)))
#     print(f'Connection {conn_id} set')
#
#
# @cli.command()
# @click.argument('conn_id')
# @click.argument('target_env', autocompletion=get_environments)
# def get_connection(conn_id, target_env):
#     config = CLIConfig(target_env)
#     try:
#         conn = config.metadata_store.get_connection(conn_id)
#         print(json.dumps(conn.__dict__, indent=4))
#     except MetadataObjectNotFound:
#         print(f'Connection "{conn_id}" not found in {target_env} metadata database {config.metadata_store.uri}')
#
#
# @cli.command()
# @click.argument('variable_id')
# @click.argument('variable_type')
# @click.argument('value')
# @click.argument('target_env', autocompletion=get_environments)
# def set_variable(variable_id, variable_type, value, target_env):
#     var = Variable(variable_id, VariableType[variable_type.upper()], value)
#     config = CLIConfig(target_env)
#     config.metadata_store.set_variable(var)
#
#
# def get_dags(ctx, args, incomplete):
#     return [k for k in [x.name for x in load_dags(settings.dags_directory())] if incomplete in k]
#
#
# def run_local_dag(dag_name: str, execution_date: datetime, env: str):
#     dag_path = Path(settings.out_directory()) / dag_name / f'{dag_name}.py'
#     if not dag_path.exists():
#         print(f"Error: {dag_path} doesn't exist. Build DAGs")
#     os.environ['TYPHOON_ENV'] = env
#     run_dag(dag_name, str(execution_date), capture_logs=False)
#
#
# @cli.command()
# @click.argument('dag_name', autocompletion=get_dags)
# @click.argument('target_env', autocompletion=get_environments)
# @click.option('--execution-date', default=None, is_flag=True, type=click.DateTime(), help='DAG execution date as YYYY-mm-dd')
# def run(dag_name: str, target_env: str, execution_date: Optional[datetime]):
#     """Run a DAG for a specific date. Will create a metadata entry in the database (TODO: create entry)."""
#     if execution_date is None:
#         execution_date = datetime.now()
#     config = CLIConfig(target_env)
#     if config.development_mode:
#         print(f'Development mode. Running {dag_name} from local build...')
#         run_local_dag(dag_name, execution_date, target_env)
#     else:
#         # TODO: Run lambda function
#         pass
#
#
# @cli.command()
# @click.argument('target_env', autocompletion=get_environments)
# def watch_dags(target_env: str):
#     """Watch changes in DAG YAMLs and build when changes are detected. Only for development mode"""
#     config = CLIConfig(target_env)
#     if not config.development_mode:
#         print('Can only watch DAGs in development mode')
#         return
#     print('Watching changes in DAGs...')
#     watch_changes(target_env)


if __name__ == '__main__':
    cli()
