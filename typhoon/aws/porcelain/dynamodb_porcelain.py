from typing import Union, Optional

from typhoon.aws.plumbing import dynamodb_plumbing
from typhoon.core import get_typhoon_config

"""Module containing higher-level functions to interact with DynamoDB
Functions are aware of the environment and use the appropriate environment variables and config files
to create the clients and resources needed.
"""


def scan_dynamodb_table(table_name: str, use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.scan_dynamodb_table(config.dynamodb_resource, table_name)


def dynamodb_table_exists(table_name: str, use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    return dynamodb_plumbing.dynamodb_table_exists(config.dynamodb_client, table_name)


def create_dynamodb_table(
        table_name: str,
        primary_key: str,
        range_key: Union[str, None] = None,  # May have other types in the future
        read_capacity_units: int = 1,
        write_capacity_units: int = 1,
        use_cli_config: bool = False,
        target_env: Optional[str] = None,
):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.create_dynamodb_table(
        ddb_client=config.dynamodb_client,
        table_name=table_name,
        primary_key=primary_key,
        range_key=range_key,
        read_capacity_units=read_capacity_units,
        write_capacity_units=write_capacity_units,
    )


def dynamodb_put_item(table_name: str, item: dict, use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    dynamodb_plumbing.dynamodb_put_item(
        ddb_client=config.dynamodb_client,
        table_name=table_name,
        item=item,
    )



