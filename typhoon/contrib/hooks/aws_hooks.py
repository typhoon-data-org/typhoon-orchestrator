from typing import Union

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

from typhoon.connections import get_connection_params
from typhoon.contrib.hooks.hook_interface import HookInterface


class AwsSessionHook(HookInterface):
    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def __enter__(self) -> boto3.session.Session:
        conn_params = get_connection_params(self.conn_id)
        self.session = boto3.session.Session(
            aws_access_key_id=conn_params.login,
            aws_secret_access_key=conn_params.password,
            region_name=conn_params.extra.get('region_name'),
        )
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DynamoDbHook(HookInterface):
    def __init__(self, conn_id: str, conn_type: str = 'client'):
        self.conn_id = conn_id
        self.conn_type = conn_type

    def __enter__(self) -> Union[BaseClient, ServiceResource]:
        conn_params = get_connection_params(self.conn_id)
        endpoint_url = None
        if conn_params.extra.get('local'):
            endpoint_url = f"http://{conn_params.login}:{conn_params.port}"
        credentials = {
            'aws_access_key_id': conn_params.login,
            'aws_secret_access_key': conn_params.password,
            'user': conn_params.login,
            'password': conn_params.password,
            'endpoint_url': endpoint_url,
            'region_name': conn_params.extra['region_name']
        }
        if self.conn_type == 'client':
            self.connection: BaseClient = boto3.client('dynamodb', **credentials)
        elif self.conn_type == 'resource':
            self.connection: int = boto3.resource('dynamodb', **credentials)
        else:
            raise ValueError(f'Expected conn_type as client or resource, found: {self.conn_type}')
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
