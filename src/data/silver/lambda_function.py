import json
import logging
from pathlib import Path
from typing import Dict

import boto3

from prepare_data import parse, json_to_pandas

BUCKET = 'estates-9036941568'
SILVER = 'data/silver'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

    # Download file from S3 and load into a pandas DF
    logging.info('Downloading - File: %s Bucket: %s' % (file_key, bucket_name))
    json_content = download_json_s3(file_key, bucket_name)
    data = {estate_id: parse(estate) for estate_id, estate in json_content.items()}
    df = json_to_pandas(data)

    # Save dataframe to the silver layer
    file_name = Path(file_key).stem
    file_path = f's3://{BUCKET}/data/silver/{file_name}.csv'
    logging.info('Saving - File: %s' % file_path)
    df.to_csv(file_path, index=False)
    return True
