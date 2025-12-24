import io
import json
import os
from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task, get_current_context, Variable
from datetime import datetime
from minio import Minio
from utils.get_env import *


# getting dates from context
def _get_covered_dates():
    context = get_current_context()['dag']
    date = context.start_date
    return date


# saving raw parquet to minio bucket
def _save_raw_parquet_to_minio(date):
    import requests
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

        print(MINIO_ENDPOINT)
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        bucket_name = MINIO_BUCKET_NAME

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
            f'Created {result.object_name} object; Etag: {result.etag}, '
            f'Version-id: {result.version_id}',
        )
        covered_dates = f'{date.year}_{date.month:02d}'
        # returning path, covered dates, size in bytes
        return result.object_name, covered_dates, bytes_size
    except Exception as err:
        print(err)
        return None


def _update_reg_table(raw_path, covered_dates, bytes_size):
    import duckdb
    with duckdb.connect(database=':memory') as con:
        con.begin()
        try:
            con.execute(f'''attach
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                as reg(type postgres, schema reg)''')

            con.execute(
                '''
                insert into reg.taxi_data (raw_path, covered_dates, file_size)
                    values (?, ?, ?)
                ''', [raw_path, covered_dates, bytes_size])

            con.commit()
            print(f'Executed\n')
            return covered_dates
        except AirflowException as err:
            print(err)
            return None


# dag initialization
@dag(
    dag_id='save_raw_data_to_minio',
    start_date=datetime(
        int(Variable.get('year')),
        int(Variable.get('month')),
        int(Variable.get('day'))
    ),
    schedule=None,
    catchup=False
)
def save_raw_data_to_minio():
    @task
    def get_covered_dates():
        return _get_covered_dates()

    @task
    def save_raw_parquet_to_minio(covered_dates):
        return _save_raw_parquet_to_minio(covered_dates)

    @task
    def update_reg_table(minio_data):
        raw_path, covered_dates, bytes_size = minio_data
        return _update_reg_table(raw_path, covered_dates, bytes_size)

    covered_dates = get_covered_dates()
    minio_data = save_raw_parquet_to_minio(covered_dates)
    update_reg_table(minio_data)


save_raw_data_to_minio()
