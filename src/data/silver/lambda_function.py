import json
import logging
from pathlib import Path
from typing import Dict
from decimal import Decimal
from datetime import datetime

import boto3
import pandas as pd

from columns import columns

BUCKET = 'estates-9036941568'
SILVER = 'data/silver'


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

    # Save preprocessed data for web to DynamoDB
    web_data = dict()
    for _id, items in data.items():
        items = {item: value for item, value in items.items() if item in columns}
        items['timestamp'] = datetime.now().strftime("%Y%m%d-%H%M%S")
        web_data[_id] = items
    logging.info('Saving DynamoDB - i% estates' % len(web_data.keys()))
    upload_json_dynamodb(web_data, table='estates')

    # Save dataframe to the silver layer
    estates = json_to_pandas(data)
    file_name = Path(file_key).stem
    file_path = f's3://{BUCKET}/data/silver/{file_name}.csv'
    logging.info('Saving (S3) - File: %s' % file_path)
    estates.to_csv(file_path, index=False)
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


def upload_json_dynamodb(data: Dict, table: str) -> bool:
    if not data:
        return False

    # Convert floats to Decimal
    # https://blog.ruanbekker.com/blog/2019/02/05/convert-float-to-decimal-data-types-for-boto3-dynamodb-using-python/
    ddb_data = json.loads(json.dumps(data), parse_float=Decimal)

    # Batch write to Dynamo
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    with table.batch_writer() as batch:
        for _id, items in ddb_data.items():
            batch.put_item(
                Item={
                    'estate_id': _id,
                    'items': items
                }
            )
    return True


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