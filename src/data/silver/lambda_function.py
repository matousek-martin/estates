import logging
from typing import Dict
from datetime import datetime

import sqlalchemy
import pandas as pd

from utils import get_secret, read_yaml


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
    estates = estates.assign(created_at=datetime.now().isoformat())

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


def fetch_max_id(engine: sqlalchemy.engine, table: str) -> int:
    """Query given table for the largest estate_id

    Args:
        engine: sqlalchemy engine
        table: name of sql table

    Returns:
        max id
    """
    query = f'''
    SELECT estate_id 
    FROM {table}
    ORDER BY estate_id DESC 
    LIMIT 1
    '''
    res = engine.execute(query)
    return res.first()[0]
