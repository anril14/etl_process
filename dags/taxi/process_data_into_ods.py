import io
import json
import os
import psycopg2
import pandas as pd
import pyarrow.parquet
from dotenv import dotenv_values
from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task, get_current_context, Variable
from datetime import datetime
from minio import Minio
from utils.get_env import *


def _check_instance(date):
    try:
        with psycopg2.connect(
                host=POSTGRES_DWH_HOST,
                port=POSTGRES_DWH_PORT,
                dbname=POSTGRES_DWH_DB,
                user=POSTGRES_DWH_USER,
                password=POSTGRES_DWH_PASSWORD,
        ) as conn:
            covered_date = f'{date.year}_{date.month:02d}'
            print(covered_date)
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    select raw_path 
                    from stg.taxi_data
                    where covered_dates = %s
                        and processed = false
                    limit 1
                    ''', (covered_date,))
                # TODO: Убрать LIMIT 1 и вызывать ошибку когда более 1 результата (сейчас для удобства разработки)
                result = cur.fetchall()
                if len(result) > 1:
                    print(f'Warning: more than 1 non-processed records for \'{covered_date}\'')
            conn.commit()
            print(f'Executed\n')
            # [][] because returns tuple inside list
            return result[0][0]
    except psycopg2.Error as err:
        print(f'psycopg2 error: {err}')
        return None


def _process_data(object_name):
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # get data of an object.
        with client.get_object(
                bucket_name=MINIO_BUCKET_NAME,
                object_name=object_name,
        ) as response:
            print(len(response.data))

    # TODO Логика обработки

    except Exception as e:
        print(e)
        raise AirflowException(e)


# dag initialization
@dag(
    dag_id='process_data_into_ods',
    start_date=datetime(
        int(Variable.get('year')),
        int(Variable.get('month')),
        int(Variable.get('day'))
    ),
    schedule=None,
    catchup=False
)
def process_data_into_ods():
    @task
    def check_instance():
        date = get_current_context()['dag'].start_date
        return _check_instance(date)

    @task
    def process_data(object_name):
        _process_data(object_name)

    object_name = check_instance()
    process_data(object_name)


process_data_into_ods()
