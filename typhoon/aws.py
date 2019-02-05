import boto3


def write_logs(body, bucket, key):
    s3 = boto3.client("s3")
    s3.put_object(Body=body, Bucket=bucket, Key=key, ContentType='text/plain')
