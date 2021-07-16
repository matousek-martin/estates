import base64
import json
from typing import Dict

import yaml
import boto3
from botocore.exceptions import ClientError


def download_json_s3(file_key: str, bucket: str) -> Dict:
    """Downloads a json file from S3

    Args:
        file_key: name of file incl. folder(s)
        bucket: S3 bucket

    Returns:
        Dict: Requested .json file
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, file_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def get_secret(secret_name: str, region_name: str) -> Dict:
    """Downloads a given secret from AWS Secrets Manager

    Args:
        secret_name: name of the secret
        region_name: AWS region

    Returns:
        secret as a json
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary,
        # only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return json.loads(secret)


def read_yaml(path: str) -> Dict:
    """Reads YAML

    Args:
        path: path to yaml with file_name.y(a)ml

    Returns:
        parsed yaml as dict
    """
    with open(path, 'r') as stream:
        file = yaml.safe_load(stream)
    return file