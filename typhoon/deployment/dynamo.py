from typing import Optional

from typhoon.aws.plumbing import dynamodb_plumbing
from typhoon.core import get_typhoon_config


def create_connections_table(use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    ddb_client = config.dynamodb_client
    table_name = config.connections_table_name

    if dynamodb_plumbing.dynamodb_table_exists(
        ddb_client=ddb_client,
        table_name=table_name,
    ):
        print(f'Table {table_name} exists. Skipping creation...')
    else:
        print(f'Creating table {table_name}...')
        dynamodb_plumbing.create_dynamodb_table(
            ddb_client=ddb_client,
            table_name=table_name,
            primary_key='conn_id',
            read_capacity_units=config.connections_table_read_capacity_units,
            write_capacity_units=config.connections_table_write_capacity_units,
        )


def create_variables_table(use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    ddb_client = config.dynamodb_client
    table_name = config.variables_table_name

    if dynamodb_plumbing.dynamodb_table_exists(
            ddb_client=ddb_client,
            table_name=table_name,
    ):
        print(f'Table {table_name} exists. Skipping creation...')
    else:
        print(f'Creating table {table_name}...')
        dynamodb_plumbing.create_dynamodb_table(
            ddb_client=ddb_client,
            table_name=table_name,
            primary_key='id',
            read_capacity_units=config.variables_table_read_capacity_units,
            write_capacity_units=config.variables_table_write_capacity_units,
        )
