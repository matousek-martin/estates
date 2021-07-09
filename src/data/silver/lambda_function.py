import json
import base64
import logging
from typing import Dict
from datetime import datetime

import boto3
import yaml
from botocore.exceptions import ClientError
import sqlalchemy
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict, _) -> bool:
    """Loads an estate from bronze (json) to silver (semi-processed dataframe)

    Args:
        event: Event that triggered the lambda function
        _: Default lambda_handler arg

    Returns:
        bool: True if successful
    """
    # Get file name and bucket of the file that triggered the lambda function
    records = [x for x in event.get('Records', []) if x.get('eventName') == 'ObjectCreated:Put']
    sorted_events = sorted(records, key=lambda e: e.get('eventTime'))
    latest_event = sorted_events[-1] if sorted_events else {}
    info = latest_event.get('s3', {})
    file_key = info.get('object', {}).get('key')
    bucket_name = info.get('bucket', {}).get('name')

    # Download file from S3
    logging.info('Downloading - File: %s Bucket: %s' % (file_key, bucket_name))
    data = download_json_s3(file_key, bucket_name)

    # Load bronze into dataframe
    estates = json_to_pandas(data)
    estates = estates.assign(created_at=datetime.now().strftime("%Y%m%d"))

    # Load RDS credentials and columns, create connection
    secret = get_secret(secret_name="estates-rds", region_name="eu-central-1")
    engine = sqlalchemy.create_engine("postgresql+psycopg2://{username}:{password}@{host}:{port}".format(**secret))
    columns = read_yaml('columns.yml')

    # Create values for ID columns
    start = fetch_max_id(engine, table='silver_estates_web') + 1
    end = start + estates.shape[0]
    estate_ids = [_id for _id in range(start, end)]
    estates = estates.assign(estate_id=estate_ids)

    # Split into tables
    logging.info('Saving (Web & Model) - %i estates' % estates.shape[0])
    model_cols = [col for col in estates.columns if col in columns['model']]
    estates.loc[:, model_cols].to_sql(name='silver_estates_model', con=engine, if_exists='append', index=False)

    web_cols = [col for col in estates.columns if col in columns['web']]
    estates.loc[:, web_cols].to_sql(name='silver_estates_web', con=engine, if_exists='append', index=False)

    images = estates.loc[:, columns['images']].explode('estate_images')
    logging.info('Saving (Images) - %i images' % images.shape[0])
    images.to_sql(name='silver_estate_images', con=engine, if_exists='append', index=False)
    return True


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


def json_to_pandas(data: Dict) -> pd.DataFrame:
    """Loads a parsed estate.json

    Args:
        data: A single estate to be parsed

    Returns:
        pd.DataFrame: A semi-processed dataframe containing relevant estate information
    """
    dataframe = pd.DataFrame.from_dict(data, orient='index')
    new_col_names = ['estate_id'] + dataframe.columns.to_list()
    dataframe = dataframe.reset_index()
    dataframe.columns = new_col_names
    return dataframe


def get_secret(secret_name: str, region_name: str) -> Dict:
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


def fetch_max_id(engine: sqlalchemy.engine, table: str) -> int:
    query = f'''
    SELECT estate_id 
    FROM {table}
    ORDER BY estate_id DESC 
    LIMIT 1
    '''
    res = engine.execute(query)
    return res.first()[0]
