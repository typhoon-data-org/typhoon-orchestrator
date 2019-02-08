from typing import Union

import boto3

from typhoon import config


def write_logs(body, bucket, key):
    s3 = boto3.client('s3')
    s3.put_object(Body=body, Bucket=bucket, Key=key, ContentType='text/plain')


def create_dynamodb_connections_table(env: str):
    ddb = connect_dynamodb_metadata(env, 'resource')
    table = ddb.create_table(
        TableName='Connections',
        KeySchema=[
            {
                'AttributeName': 'conn_id',
                'KeyType': 'HASH'  # Partition key
            },
            # {
            #     'AttributeName': 'title',
            #     'KeyType': 'RANGE'  # Sort key
            # }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'conn_id',
                'AttributeType': 'S'
            },
            # {
            #     'AttributeName': 'conn_type',
            #     'AttributeType': 'S'
            # },
            # {
            #     'AttributeName': 'host',
            #     'AttributeType': 'S'
            # },
            # {
            #     'AttributeName': 'port',
            #     'AttributeType': 'N'
            # },
            # {
            #     'AttributeName': 'login',
            #     'AttributeType': 'S'
            # },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def connect_dynamodb_metadata(env: str, conn_type: str = 'resource'):
    endpoint_url = config.get(env, 'dynamodb-endpoint', '')
    extra_params = {}
    if endpoint_url:
        extra_params = {
            'aws_access_key_id': 'dummy',
            'aws_secret_access_key': 'dummy',
            'endpoint_url': endpoint_url,
            'region_name': 'us-west-2',
        }

    if conn_type == 'client':
        ddb = boto3.client('dynamodb', **extra_params)
    elif conn_type == 'resource':
        ddb = boto3.resource('dynamodb', **extra_params)
    else:
        raise ValueError(f'Expected conn_type as client or resource, found: {conn_type}')
    return ddb


if __name__ == '__main__':
    t = create_dynamodb_connections_table('dev')
    a = 2
