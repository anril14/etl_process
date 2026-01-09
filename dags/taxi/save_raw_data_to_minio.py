import io
import json
import os
from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task, get_current_context, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.task.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from minio import Minio
from utils.get_env import *


# getting dates from context
def _get_covered_dates():
    context = get_current_context()
    conf = context['dag_run'].conf
    if conf and 'year' in conf and 'month' in conf:
        month = int(conf['month'])
        year = int(conf['year'])
        return datetime(year, month, 1)

    date = context['data_interval_start']
    if date.month == 1:
        return datetime(date.year - 1, 11, 1)
    if date.month == 2:
        return datetime(date.year - 1, 12, 1)
    return datetime(date.year, date.month - 2, 1)


def _check_instance(date):
    import duckdb
    print(f'result date: {date}')
    with duckdb.connect(database=':memory') as con:
        con.execute(f'''ATTACH
            'host={POSTGRES_DWH_HOST} 
            port={POSTGRES_DWH_PORT}
            dbname={POSTGRES_DWH_DB} 
            user={POSTGRES_DWH_USER} 
            password={POSTGRES_DWH_PASSWORD}'
            AS reg(TYPE postgres, SCHEMA reg)''')

        covered_date = f'{date.year}_{date.month:02d}'
        print(f'covered_date: {covered_date}')
        result = con.execute(
            '''
            SELECT raw_path 
            FROM reg.taxi_data
            WHERE covered_dates = ?
                AND processed = FALSE
            ''', [covered_date]).fetchall()

        print(f'result: {result}')

        if len(result) == 0:
            return 'save_raw_parquet_to_minio'

        return 'trigger_processing'


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
            con.execute(f'''ATTACH
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                AS reg(TYPE postgres, SCHEMA reg)''')

            con.execute(
                '''
                INSERT INTO reg.taxi_data (raw_path, covered_dates, file_size)
                    VALUES (?, ?, ?)
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
    catchup=False,
    schedule='0 0 1 * *',
)
def save_raw_data_to_minio():
    @task
    def get_covered_dates():
        return _get_covered_dates()

    @task
    def prepare_conf(covered_date):
        return {
            'year': covered_date.year,
            'month': covered_date.month
        }

    @task
    def save_raw_parquet_to_minio(covered_dates):
        return _save_raw_parquet_to_minio(covered_dates)

    @task
    def update_reg_table(minio_data):
        raw_path, covered_dates, bytes_size = minio_data
        return _update_reg_table(raw_path, covered_dates, bytes_size)

    covered_dates = get_covered_dates()
    conf_dates = prepare_conf(covered_dates)

    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing',
        trigger_dag_id='process_data_into_ods',
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
        conf=conf_dates
    )

    branch_task = BranchPythonOperator(
        task_id='check_instance',
        python_callable=_check_instance,
        op_args=[covered_dates]
    )

    minio_data = save_raw_parquet_to_minio(covered_dates)
    update = update_reg_table(minio_data)

    covered_dates >> branch_task
    branch_task >> [trigger_processing, minio_data]
    [minio_data] >> update
    update >> trigger_processing


save_raw_data_to_minio()
