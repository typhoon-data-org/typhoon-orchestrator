import boto3

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
