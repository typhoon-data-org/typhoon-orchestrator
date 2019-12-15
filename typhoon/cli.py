import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import click
import yaml
from dataclasses import asdict
from termcolor import colored

from typhoon import connections
from typhoon.connections import Connection
from typhoon.core import settings
from typhoon.core.config import CLIConfig, Config
from typhoon.core.dags import DagDeployment
from typhoon.core.glue import transpile_dag_and_store, load_dags, get_dags_contents
from typhoon.core.metadata_store_interface import MetadataObjectNotFound
from typhoon.core.settings import out_directory
from typhoon.deployment.dags import get_dag_filenames
from typhoon.deployment.deploy import deploy_dag_requirements, copy_local_typhoon, copy_user_defined_code
from typhoon.handler import run_dag
from typhoon.metadata_store_impl import MetadataStoreType
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
    pass


def dags_with_changes() -> List[str]:
    result = []
    for dag_file in get_dag_filenames():
        yaml_path = Path(settings.dags_directory()) / dag_file
        yaml_modified_ts = datetime.fromtimestamp(yaml_path.stat().st_mtime)
        dag_name = yaml.safe_load(yaml_path.read_text())['name']
        transpiled_path = Path(settings.out_directory()) / dag_name / f'{dag_name}.py'
        transpiled_created_ts = datetime.fromtimestamp(transpiled_path.stat().st_ctime)
        if yaml_modified_ts > transpiled_created_ts:
            result.append(dag_name)

    return result


def dags_without_deploy(env) -> List[str]:
    undeployed_dags = []
    config = CLIConfig(env)
    for dag_code in get_dags_contents(settings.dags_directory()):
        loaded_dag = yaml.safe_load(dag_code)
        dag_deployment = DagDeployment(loaded_dag['name'], deployment_date=datetime.utcnow(), dag_code=dag_code)
        if loaded_dag.get('active', True):
            try:
                _ = config.metadata_store.get_dag_deployment(dag_deployment.deployment_hash)
            except MetadataObjectNotFound:
                undeployed_dags.append(dag_deployment.dag_name)
    return undeployed_dags


def get_environments(ctx, args, incomplete):
    config_path = 'cliconfig.cfg'
    config = Config(config_path, env='')
    return [k for k in [x.lower() for x in config.config.keys()] if incomplete in k]


def get_undefined_connections_in_metadata_db(config: CLIConfig, conn_ids: List[str]):
    undefined_connections = []
    for conn_id in conn_ids:
        try:
            config.metadata_store.get_connection(conn_id)
        except MetadataObjectNotFound:
            undefined_connections.append(conn_id)
    return undefined_connections


def get_undefined_variables_in_metadata_db(config: CLIConfig, var_ids: List[str]):
    undefined_variables = []
    for var_id in var_ids:
        try:
            config.metadata_store.get_variable(var_id)
        except MetadataObjectNotFound:
            undefined_variables.append(var_id)
    return undefined_variables


def check_connections_yaml(config: CLIConfig, env: str):
    if not Path('connections.yml').exists():
        print(colored('• Connections YAML not found. For better version control create', 'red'), colored('connections.yml', 'grey'))
        print(colored('  Skipping connections YAML checks...', 'red'))
        return
    conn_yml = yaml.safe_load(Path('connections.yml').read_text())
    undefined_connections = get_undefined_connections_in_metadata_db(config, conn_ids=conn_yml.keys())
    if undefined_connections:
        print(colored('• Found connections in YAML that are not defined in the metadata database', 'yellow'))
        for conn_id in undefined_connections:
            print(
                colored('   - Connection', 'yellow'),
                colored(conn_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon set-connection {conn_id} CONN_ENV {env}', 'grey')
            )
    else:
        print(colored('• All connections in YAML are defined in the database', 'green'))


def check_connections_dags(config: CLIConfig, env: str):
    all_conn_ids = set()
    for dag_file in Path(settings.dags_directory()).rglob('*.yml'):
        conn_ids = re.findall(r'\$HOOK\.(\w+)', dag_file.read_text())
        all_conn_ids = all_conn_ids.union(conn_ids)
    undefined_connections = get_undefined_connections_in_metadata_db(config, conn_ids=all_conn_ids)
    if undefined_connections:
        print(colored('• Found connections in DAGs that are not defined in the metadata database', 'yellow'))
        for conn_id in undefined_connections:
            print(
                colored('   - Connection', 'yellow'),
                colored(conn_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon set-connection {conn_id} CONN_ENV {env}', 'grey')
            )
    else:
        print(colored('• All connections in the DAGs are defined in the database', 'green'))


def check_variables_dags(config: CLIConfig, env: str):
    all_var_ids = set()
    for dag_file in Path(settings.dags_directory()).rglob('*.yml'):
        var_ids = re.findall(r'\$VARIABLE\.(\w+)', dag_file.read_text())
        all_var_ids = all_var_ids.union(var_ids)
    undefined_variables = get_undefined_variables_in_metadata_db(config, var_ids=all_var_ids)
    if undefined_variables:
        print(colored('• Found variables in DAGs that are not defined in the metadata database', 'yellow'))
        for var_id in undefined_variables:
            print(
                colored('   - Variable', 'yellow'),
                colored(var_id, 'blue'),
                colored('is not set. Try', 'yellow'),
                colored(f'typhoon set-variable {var_id} VAR_TYPE VALUE {env}', 'grey')
            )
    else:
        print(colored('• All variables in the DAGs are defined in the database', 'green'))


@cli.command()
@click.argument('env', autocompletion=get_environments)
def status(env):
    print(colored(ascii_art_logo, 'cyan'))
    if 'TYPHOON_HOME' not in os.environ.keys():
        print(
            colored('• Typhoon home not set... To define in current directory run', 'red'),
            colored('To define in current directory run', 'white'),
            colored('export TYPHOON_HOME=$(pwd)', 'grey'))
        print(colored('Aborting checks...', 'red'))
        return
    else:
        print(colored('• TYPHOON_HOME set to', 'green'), colored(os.environ['TYPHOON_HOME'], 'grey'))

    if not Path('cliconfig.cfg').exists():
        print(colored('CLI config file not found! Create one called', 'red'), colored('cliconfig.cfg', 'grey'))
        print(colored('Aborting checks...', 'red'))
        return

    config = CLIConfig(env)
    if config.aws_profile:
        print(colored('• Using AWS profile', 'green'), colored(config.aws_profile, 'grey'))
    elif config.development_mode:
        print(colored('• Dev mode, skipping AWS profile check...', 'green'))
    else:
        print(colored('• No AWS profile found. Add to cliconfig.cfg...', 'red'))

    if config.metadata_store.exists():
        print(colored('• Metadata database found in', 'green'), colored(config.metadata_store.uri, 'grey'))
        check_connections_yaml(config, env)
        check_connections_dags(config, env)
        check_variables_dags(config, env)
    elif config.metadata_store_type == MetadataStoreType.sqlite:
        print(colored('• Metadata store not found for', 'yellow'), colored(config.metadata_store.uri, 'grey'))
        print(colored('   - It will be created upon use, or create by running (idempotent) command', color='blue'), colored(f'typhoon migrate {env}', 'grey'))
        print(colored('  Skipping connections and variables checks...', 'red'))
    else:
        print(colored('• Metadata store not found or incomplete for', 'red'), colored(config.metadata_store.uri, 'grey'))
        print(colored('   - Fix by running (idempotent) command', color='blue'), colored(f'typhoon migrate {env}', 'grey'))
        print(colored('  Skipping connections and variables checks...', 'red'))

    if config.development_mode:
        changed_dags = dags_with_changes()
        if changed_dags:
            print(colored('• Unbuilt changes in DAGs...', 'yellow'), colored('To rebuild run', 'white'),
                  colored(f'typhoon build-dags {env} [--debug]', 'grey'))
            for dag in changed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))
    else:
        undeployed_dags = dags_without_deploy(env)
        if undeployed_dags:
            print(colored('• Undeployed changes in DAGs...', 'yellow'), colored('To deploy run', 'white'),
                  colored(f'typhoon deploy-dags {env} [--build-dependencies]', 'grey'))
            for dag in undeployed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))


@cli.command()
@click.argument('target_env')
def migrate(target_env):
    """Add the necessary IAM roles and DynamoDB tables"""
    from typhoon.deployment.iam import deploy_role
    deploy_role(use_cli_config=True, target_env=target_env)

    config = CLIConfig(target_env)
    config.metadata_store.migrate()


@cli.command()
@click.argument('target_env')
def clean(target_env):
    from typhoon.deployment.iam import clean_role
    clean_role(use_cli_config=True, target_env=target_env)


@cli.command()
@click.argument('target_env')
@click.option('--debug', default=False, is_flag=True)
def build_dags(target_env, debug):
    """Build code for dags in $TYPHOON_HOME/out/"""
    print(ascii_art_logo)
    build_all_dags(target_env, debug)


def build_all_dags(target_env, debug):
    from typhoon.deployment.deploy import clean_out
    from typhoon.deployment.sam import deploy_sam_template

    clean_out()

    config = CLIConfig(target_env)

    print('Build all DAGs...')
    dags = load_dags(settings.dags_directory())
    deploy_sam_template(dags, use_cli_config=True, target_env=target_env)
    for dag in dags:
        dag = dag.as_dict()
        dag_folder = Path(settings.out_directory()) / dag['name']
        transpile_dag_and_store(dag, dag_folder / f"{dag['name']}.py", env=target_env, debug_mode=debug)
        if debug and config.metadata_store_type == MetadataStoreType.sqlite:
            local_store_path = Path(settings.typhoon_home()) / 'project.db'
            if not config.metadata_store.exists():
                print(f'No sqlite store found. Creating in {local_store_path}...')
                open(str(local_store_path), 'a').close()
            print(f'Setting up database in {local_store_path} as symlink for persistence...')
            os.symlink(str(local_store_path), dag_folder / 'project.db')

        deploy_dag_requirements(dag, config.typhoon_version_is_local, config.typhoon_version)
        if config.typhoon_version_is_local:
            copy_local_typhoon(dag, config.typhoon_version)

        if debug:
            print('Setting up user defined code as symlink for debugging...')
        copy_user_defined_code(dag, symlink=debug)

    if debug and config.metadata_store_type == MetadataStoreType.sqlite:
        local_store_path = Path(settings.typhoon_home()) / f'{config.project_name}.db'
        if local_store_path.exists():
            os.symlink(str(local_store_path), Path(settings.out_directory()) / f'{config.project_name}.db')

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


@cli.command()
@click.argument('target_env')
@click.option('--build-dependencies', default=False, is_flag=True, help='Build DAG dependencies in Docker container')
def deploy_dags(target_env, build_dependencies):
    from typhoon.core import get_typhoon_config

    config = get_typhoon_config(use_cli_config=True, target_env=target_env)

    build_all_dags(target_env=target_env, debug=False)
    if build_dependencies:
        run_in_subprocess(f'sam build --use-container', cwd=out_directory())
    build_dir = str(Path(out_directory()) / '.aws-sam' / 'build')
    run_in_subprocess(
        f'sam package --template-file template.yaml --s3-bucket {config.s3_bucket} --profile {config.aws_profile}'
        f' --output-template-file out_template.yaml',
        cwd=build_dir
    )
    run_in_subprocess(
        f'sam deploy --template-file out_template.yaml '
        f'--stack-name {config.project_name.replace("_", "-")} --profile {config.aws_profile} '
        f'--region {config.deploy_region} --capabilities CAPABILITY_IAM',
        cwd=build_dir
    )

    if not config.development_mode:
        for dag_code in get_dags_contents(settings.dags_directory()):
            loaded_dag = yaml.safe_load(dag_code)
            if loaded_dag.get('active', True):
                dag_deployment = DagDeployment(loaded_dag['name'], deployment_date=datetime.utcnow(), dag_code=dag_code)
                config.metadata_store.set_dag_deployment(dag_deployment)


@cli.command()
@click.argument('conn_id')
@click.argument('conn_env')
@click.argument('target_env')
def set_connection(conn_id, conn_env, target_env):
    conn_params = connections.get_connection_local(conn_id, conn_env)
    config = CLIConfig(target_env)
    config.metadata_store.set_connection(Connection(conn_id=conn_id, **asdict(conn_params)))
    print(f'Connection {conn_id} set')


@cli.command()
@click.argument('conn_id')
@click.argument('target_env')
def get_connection(conn_id, target_env):
    config = CLIConfig(target_env)
    try:
        conn = config.metadata_store.get_connection(conn_id)
        print(json.dumps(conn.__dict__, indent=4))
    except MetadataObjectNotFound:
        print(f'Connection "{conn_id}" not found in {target_env} metadata database {config.metadata_store.uri}')


@cli.command()
@click.argument('variable_id')
@click.argument('variable_type')
@click.argument('value')
@click.argument('target_env')
def set_variable(variable_id, variable_type, value, target_env):
    var = Variable(variable_id, VariableType[variable_type.upper()], value)
    config = CLIConfig(target_env)
    config.metadata_store.set_variable(var)


def get_dags(ctx, args, incomplete):
    return [k for k in [x.name for x in load_dags(settings.dags_directory())] if incomplete in k]


def run_local_dag(dag_name: str, execution_date: datetime, env: str):
    dag_path = Path(settings.out_directory()) / dag_name / f'{dag_name}.py'
    if not dag_path.exists():
        print(f"Error: {dag_path} doesn't exist. Build DAGs")
    os.environ['TYPHOON_ENV'] = env
    run_dag(dag_name, str(execution_date), capture_logs=False)


@cli.command()
@click.argument('dag_name', autocompletion=get_dags)
@click.argument('target_env', autocompletion=get_environments)
@click.option('--execution-date', default=None, is_flag=True)
def run(dag_name: str, target_env: str, execution_date: Optional[datetime]):
    if execution_date is None:
        execution_date = datetime.now()
    config = CLIConfig(target_env)
    if config.development_mode:
        print(f'Development mode. Running {dag_name} from local build...')
        run_local_dag(dag_name, execution_date, target_env)
    else:
        # Run lambda function
        pass


@cli.command()
@click.argument('target_env', autocompletion=get_environments)
def watch_dags(target_env: str):
    config = CLIConfig(target_env)
    if not config.development_mode:
        print('Can only watch DAGs in development mode')
        return
    print('Watching changes in DAGs...')
    watch_changes(target_env)


if __name__ == '__main__':
    cli()
