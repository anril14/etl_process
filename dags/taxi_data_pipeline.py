import io
import json
import os
import psycopg2
import pandas as pd
import pyarrow.parquet
from airflow.sdk.bases.operator import AirflowException
from dotenv import dotenv_values
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from random import randint
from minio import Minio
import requests


# saving raw parquet to minio bucket
def _save_raw_to_minio(date):
    try:
        url = (f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
               f'{date.year}-{date.month:02d}.parquet')
        print(url)
        r = requests.get(url=url)
        print(len(r.content))
    except Exception:
        raise Exception('Get request error')

    try:
        # get size in bytes
        bytes_size = len(r.content)

        print(os.getenv('MINIO_ENDPOINT'))
        client = Minio(
            endpoint=str(os.getenv('MINIO_ENDPOINT')),
            access_key=str(os.getenv('MINIO_ACCESS_KEY')),
            secret_key=str(os.getenv('MINIO_SECRET_KEY')),
            secure=False
        )
        bucket_name = str(os.getenv('MINIO_BUCKET_NAME'))

        # create bucket if no one
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Bucket {bucket_name} is created, saving file...')
        else:
            print(f'Bucket {bucket_name} is already created, saving file...')

        result = client.put_object(bucket_name=bucket_name,
                                   object_name=f'raw/taxi/{date.year}/{date.month:02d}/yellow_tripdata.parquet',
                                   data=io.BytesIO(r.content),
                                   content_type='application/octet-stream',
                                   length=bytes_size,
                                   )
        # logs
        print(
            f'created {result.object_name} object; etag: {result.etag}, '
            f'version-id: {result.version_id}',
        )
        covered_dates = f'{date.year}_{date.month}'
        # returning path, covered dates, size in bytes
        return result.object_name, covered_dates, bytes_size
    except Exception as e:
        print(e)
        raise AirflowException(e)


def _stg_update(raw_path, covered_dates, bytes_size):
    with psycopg2.connect(
            host=os.getenv('POSTGRES_DWH_HOST'),
            port=os.getenv('POSTGRES_DWH_PORT'),
            dbname=os.getenv('POSTGRES_DWH_DB'),
            user=os.getenv('POSTGRES_DWH_USER'),
            password=os.getenv('POSTGRES_DWH_PASSWORD'),
    ) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    insert into stg.taxi_data (raw_path, covered_dates, file_size)
                    values (%s, %s, %s)
                    ''', (raw_path, covered_dates, bytes_size))
        except Exception as e:
            print(e)
            raise ConnectionError
        conn.commit()
        print(f'executed')


# TODO Валидация и трансфер данных в ODS

# dag initialization
@dag(
    dag_id='taxi_data_pipeline',
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False
)
def taxi_data_pipeline():
    @task
    def save_raw_to_minio():
        date = get_current_context()['dag'].start_date
        return _save_raw_to_minio(date)

    @task
    def stg_update(minio_data):
        raw_path, covered_dates, bytes_size = minio_data
        _stg_update(raw_path, covered_dates, bytes_size)

    minio_data = save_raw_to_minio()
    stg_update(minio_data)


taxi_data_pipeline()
