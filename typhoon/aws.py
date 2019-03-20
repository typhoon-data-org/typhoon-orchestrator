import decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer

from typhoon import config


def write_logs(body, bucket, key):
    s3 = boto3.client('s3')
    s3.put_object(Body=body, Bucket=bucket, Key=key, ContentType='text/plain')


def dynamodb_table_exists(env: str, table: str):
    ddb = connect_dynamodb_metadata(env, 'client')
    existing_tables = ddb.list_tables()['TableNames']
    return table in existing_tables


def create_dynamodb_connections_table(env: str):
    ddb = connect_dynamodb_metadata(env, 'resource')
    table = ddb.create_table(
        TableName='Connections',
        KeySchema=[
            {
                'AttributeName': 'conn_id',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'conn_id',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def create_dynamodb_variables_table(env: str):
    ddb = connect_dynamodb_metadata(env, 'resource')
    table = ddb.create_table(
        TableName='Variables',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def create_dynamodb_dags_table(env: str):
    ddb = connect_dynamodb_metadata(env, 'resource')
    table = ddb.create_table(
        TableName='Dags',
        KeySchema=[
            {
                'AttributeName': 'dag_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'execution_date',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'conn_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'execution_date',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def connect_dynamodb_metadata(env: str, conn_type: str = 'resource'):
    aws_profile = config.get(env, 'aws-profile')
    endpoint_url = config.get(env, 'dynamodb-endpoint')
    aws_region = config.get(env, 'aws-region')
    extra_params = {'region_name': aws_region}
    if endpoint_url:
        extra_params = {
            'aws_access_key_id': 'dummy',
            'aws_secret_access_key': 'dummy',
            'endpoint_url': endpoint_url,
        }

    if aws_profile:
        session = boto3.session.Session(profile_name=aws_profile)
    else:
        session = boto3

    if conn_type == 'client':
        ddb = session.client('dynamodb', **extra_params)
    elif conn_type == 'resource':
        ddb = session.resource('dynamodb', **extra_params)
    else:
        raise ValueError(f'Expected conn_type as client or resource, found: {conn_type}')
    return ddb


def scan_dynamodb_table(env: str, table_name: str):
    ddb = connect_dynamodb_metadata(env, 'resource')
    table = ddb.Table(table_name)
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = replace_decimals(v)
        return obj
    elif isinstance(obj, set):
        return set(replace_decimals(i) for i in obj)
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj
