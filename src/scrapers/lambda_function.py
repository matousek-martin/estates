import json
import logging
from datetime import datetime

import boto3

from scraper import start_requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_newest_file(bucket: str, prefix: str) -> str:
    """Fetches the file name of the file with the newest timestamp in the specified bucket

    Args:
        bucket: S3 bucket containing the file
        prefix: e.g. folder(s)

    Returns:
        str: File name
    """
    s3 = boto3.client(service_name='s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    sorted_response = sorted(response['Contents'], key=lambda x: x['LastModified'])
    file_path = sorted_response.pop()['Key']
    return file_path.strip(prefix)


def lambda_handler(event, context) -> bool:
    """Scrapes latest estates from sreality and uploads them to S3 bucket as json

    Args:
        event: default lambda param
        context: default lambda param

    Returns:
        bool: returns True if successful
    """
    bucket = 'estates-9036941568'
    folder = 'raw/'

    newest_file = get_newest_file(bucket, folder)
    last_estate_id = newest_file.strip('.json').split('_')[-1]

    estates = start_requests(last_estate_id)

    newest_estate_id = list(estates.keys())[0]
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f'{now}_{newest_estate_id}.json'
    file = json.dumps(estates)

    logging.info('Saving estates to S3')
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).put_object(Key=folder + file_name, Body=file)
    # TODO: Handle errors better
    return True
