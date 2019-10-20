import os
import subprocess
from pathlib import Path

import click
from dataclasses import asdict

from typhoon import connections
from typhoon.connections import Connection
from typhoon.core import settings
from typhoon.core.config import CLIConfig
from typhoon.core.glue import transpile_dag_and_store, load_dags
from typhoon.core.settings import out_directory
from typhoon.deployment.deploy import deploy_dag_requirements, copy_local_typhoon, copy_user_defined_code
from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.variables import Variable, VariableType


@click.group()
def cli():
    """Typhoon CLI"""
    pass


@cli.command()
@click.argument('target_env')
def migrate(target_env):
    """Add the necessary IAM roles and DynamoDB tables"""
    from typhoon.deployment.iam import deploy_role
    deploy_role(use_cli_config=True, target_env=target_env)

    from typhoon.deployment.dynamo import create_connections_table, create_variables_table
    create_connections_table(use_cli_config=True, target_env=target_env)
    create_variables_table(use_cli_config=True, target_env=target_env)


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
    _build_dags(target_env, debug)


def _build_dags(target_env, debug):
    from typhoon.deployment.deploy import clean_out
    from typhoon.deployment.sam import deploy_sam_template

    clean_out()

    config = CLIConfig(target_env)

    dags = load_dags(settings.dags_directory())
    deploy_sam_template(dags, use_cli_config=True, target_env=target_env)
    for dag in dags:
        dag = dag.as_dict()
        dag_folder = Path(settings.out_directory())/dag['name']
        transpile_dag_and_store(dag, dag_folder/f"{dag['name']}.py", env=target_env, debug_mode=debug)
        if debug and config.metadata_store_type == MetadataStoreType.sqlite:
            local_store_path = Path(settings.typhoon_home()) / 'project.db'
            if local_store_path.exists():
                os.symlink(str(local_store_path), dag_folder / 'project.db')

        deploy_dag_requirements(dag, config.typhoon_version_is_local(), config.typhoon_version)
        if config.typhoon_version_is_local():
            copy_local_typhoon(dag, config.typhoon_version)

        copy_user_defined_code(dag)

    if debug and config.metadata_store_type == MetadataStoreType.sqlite:
        local_store_path = Path(settings.typhoon_home()) / 'project.db'
        if local_store_path.exists():
            os.symlink(str(local_store_path), Path(settings.out_directory())/'project.db')


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
@click.option('--build-dependencies', default=False, is_flag=True)
def deploy_dags(target_env, build_dependencies):
    from typhoon.core import get_typhoon_config

    config = get_typhoon_config(use_cli_config=True, target_env=target_env)

    _build_dags(target_env=target_env, debug=False)
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


@cli.command()
@click.argument('conn_id')
@click.argument('conn_env')
@click.argument('target_env')
def set_connection(conn_id, conn_env, target_env):
    conn_params = connections.get_connection_local(conn_id, conn_env)
    config = CLIConfig(target_env)
    config.metadata_store.set_connection(Connection(conn_id=conn_id, **asdict(conn_params)))


@cli.command()
@click.argument('variable_id')
@click.argument('variable_type')
@click.argument('value')
@click.argument('target_env')
def set_variable(variable_id, variable_type, value, target_env):
    var = Variable(variable_id, VariableType[variable_type.upper()], value)
    config = CLIConfig(target_env)
    config.metadata_store.set_variable(var)


if __name__ == '__main__':
    cli()
