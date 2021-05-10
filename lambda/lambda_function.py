import json
import logging
from datetime import datetime

import boto3

from scraper import start_requests, get_newest_file

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
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
