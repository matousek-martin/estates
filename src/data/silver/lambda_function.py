import time
import json
import logging
from pathlib import Path
from typing import Dict, List
from decimal import Decimal
from datetime import datetime


import boto3
import pandas as pd

from columns import columns

BUCKET = 'estates-9036941568'
SILVER = 'data/silver'

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

    # Save preprocessed data for web to DynamoDB
    records = bronze_to_records(data, columns)
    logging.info('Saving DynamoDB - %i estates' % len(records))
    upload_json_dynamodb(records, table='estates', chunk_size=50)

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


def bronze_to_records(data: Dict, columns: List[str]) -> List[Dict]:
    """Transforms data from bronze into records suitable for DynamoDB

    Args:
        data: scraped estates from bronze layer
        columns: a list of columns picked for web

    Returns:
        list of estates (Dicts) for DynamoDB
    """
    records = list()
    for estate_id, items in data.items():
        record = dict()
        for column, value in items.items():
            if column in columns:
                record[column] = value
        record['estate_id'] = estate_id
        # Convert floats to Decimal
        # https://blog.ruanbekker.com/blog/2019/02/05/convert-float-to-decimal-data-types-for-boto3-dynamodb-using-python/
        record = json.loads(json.dumps(record), parse_float=Decimal)
        records.append(record)
    return records


def upload_json_dynamodb(records: List[Dict], table: str, chunk_size: int) -> bool:
    """Writes newly added estates to DynamoDB table

    Args:
        records: a list of estates
        table: AWS DynamoDB table name
        chunk_size: split records into chunks of size 'chunk_size' to avoid put_item size limits

    Returns:
        (bool): success True/False
    """
    # Skip if there are not new estates to upload
    if not records:
        return False

    # Write to Dynamo
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    with table.batch_writer() as batch:
        for i in range(0, len(records), chunk_size):
            batch.put_item(Item={
                # Iso format for creation timestamp
                # Unix for time to live (expire after 30 days)
                # https://stackoverflow.com/questions/40561484/what-data-type-should-be-use-for-timestamp-in-dynamodb
                # https://jun711.github.io/aws/how-to-set-ttl-for-amazon-dynamodb-entries/
                'created_at': datetime.now().replace(microsecond=0).isoformat(),
                'expires_at': int(time.time() + 30 * 24 * 60 * 60),
                'estates': records[i: i+chunk_size]
            })
            time.sleep(1)
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