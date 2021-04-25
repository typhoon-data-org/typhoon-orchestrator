import boto3

from typhoon.connections import ConnectionParams
from typhoon.contrib.hooks.hook_interface import HookInterface


class AwsSessionHook(HookInterface):
    def __init__(self, conn_params: ConnectionParams):
        self.conn_params = conn_params
        self.session = None

    def __enter__(self):
        profile = self.conn_params.extra.get('profile')
        if profile:
            self.session = boto3.session.Session(profile_name=profile)
        self.session = boto3.session.Session(
            aws_access_key_id=self.conn_params.login,
            aws_secret_access_key=self.conn_params.password,
            region_name=self.conn_params.extra.get('region_name'),
        )
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session = None


class DynamoDbHook(HookInterface):
    conn_type = 'dynamodb'

    def __init__(self, conn_params: ConnectionParams, conn_type: str = 'client'):
        self.conn_params = conn_params
        self.conn_type = conn_type

    def __enter__(self):
        import boto3

        conn_params = self.conn_params
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
            self.connection = boto3.client('dynamodb', **credentials)
        elif self.conn_type == 'resource':
            self.connection = boto3.resource('dynamodb', **credentials)
        else:
            raise ValueError(f'Expected conn_type as client or resource, found: {self.conn_type}')
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection = None
