import click

from typhoon.core import get_typhoon_config
from typhoon.deployment.dynamo import create_connections_table, create_variables_table
from typhoon.deployment.iam import deploy_role, clean_role


@click.group()
def cli():
    """Typhoon CLI"""
    pass


@cli.command()
@click.argument('target_env')
def migrate(target_env):
    """Add the necessary IAM roles and DynamoDB tables"""
    deploy_role(use_cli_config=True, target_env=target_env)

    create_connections_table(use_cli_config=True, target_env=target_env)
    create_variables_table(use_cli_config=True, target_env=target_env)


@cli.command()
@click.argument('target_env')
def clean(target_env):
    clean_role(use_cli_config=True, target_env=target_env)


@cli.command()
@click.argument('target_env')
def build_dags(target_env):
    """Build code for dags in $TYPHOON_HOME/out/"""
    config = get_typhoon_config(use_cli_config=True, target_env=target_env)

    from typhoon.deployment.deploy import clean_out
    clean_out()

    from typhoon.deployment.dags import load_dags
    dags = load_dags()
    for dag in dags:
        from typhoon.deployment.deploy import build_dag_code
        build_dag_code(dag, target_env)

    from typhoon.deployment.deploy import copy_user_defined_code
    copy_user_defined_code()


if __name__ == '__main__':
    cli()
