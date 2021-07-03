import json
import logging
from datetime import datetime
from typing import Dict

import boto3

from scraper import get_estate_urls, start_requests

BUCKET = 'estates-9036941568'
BRONZE = 'data/bronze'

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


def lambda_handler(event: Dict, _) -> bool:
    """Scrapes latest estates from sreality and uploads them to S3 bucket as json

    Args:
        event: default lambda param
        _: default lambda param

    Returns:
        bool: returns True if successful
    """
    # Get ID of estate from previous scarpe
    newest_file = get_newest_file(BUCKET, BRONZE)
    last_estate_id = newest_file.strip('.json').split('_')[-1]

    # Scrape new estates
    estate_urls = get_estate_urls(last_estate_id)
    estates = start_requests(estate_urls)
    if not estates:
        return False

    # File naming convention - Ymd-hms_<estate_id>
    newest_estate_id = list(estates.keys())[0]
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f'{now}_{newest_estate_id}.json'
    file = json.dumps(estates)

    # Save to S3 bucket
    logging.info('Saving estates to S3')
    s3 = boto3.resource('s3')
    s3.Bucket(BUCKET).put_object(Key=BRONZE + '/' + file_name, Body=file)
    return True
