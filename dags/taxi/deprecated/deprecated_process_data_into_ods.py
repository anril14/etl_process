import io
import json
import os

import duckdb
import psycopg2
import pyarrow.parquet
from dotenv import dotenv_values
from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task, get_current_context, Variable
from datetime import datetime
from minio import Minio
from utils import get_sql, get_env


# getting dates from context
def _d_get_covered_dates():
    context = get_current_context()
    conf = context['dag_run'].conf
    if conf and conf['year'] and conf['month']:
        print(f'conf {conf}')

        year = int(conf['year'])
        month = int(conf['month'])

        date = datetime(year, month, 1)
        return date
    else:
        raise ValueError('Invalid date format')

def _d_check_instance(date):
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
                    from reg.taxi_data
                    where covered_dates = %s
                        and processed = false
                    limit 1
                    ''', (covered_date,))
                result = cur.fetchall()
                if len(result) > 1:
                    print(f'Warning: more than 1 non-processed records for \'{covered_date}\'')
            conn.commit()
            print(f'Executed\n')
            print(result)
            # [][] because returns tuple inside list
            return result[0][0], date.year
    except psycopg2.Error as err:
        print(f'psycopg2 error: {err}')
        raise TypeError(err)


def _d_process_data(object_name, year, batch_size):
    import pandas as pd
    import io
    from minio import Minio
    import psycopg2
    from sqlalchemy import create_engine
    from airflow.exceptions import AirflowException

    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        with client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=object_name) as response:
            # Подключение к хранилищу MinIO
            print(f'Bytes length: {len(response.data)}')

            df = pd.read_parquet(io.BytesIO(response.data))
            df = df.head(100_000)
            print(df.head())

            from utils.columns import ODS_COLUMN_MAPPING
            df = df.rename(columns=ODS_COLUMN_MAPPING)

            df_valid = df[
                df['vendor_id'].notnull() &
                df['tpep_pickup'].notnull() &
                df['tpep_dropoff'].notnull() &
                df['passenger_count'].notnull() &
                df['trip_distance'].notnull()
                ]

            df_valid['cbd_congestion_fee'] = df_valid.get('cbd_congestion_fee', pd.NA)
            buffer = io.StringIO()
            df_valid.to_csv(buffer, sep='\t', header=False, index=False, na_rep='')
            buffer.seek(0)

            with psycopg2.connect(
                    host=POSTGRES_DWH_HOST,
                    port=POSTGRES_DWH_PORT,
                    dbname=POSTGRES_DWH_DB,
                    user=POSTGRES_DWH_USER,
                    password=POSTGRES_DWH_PASSWORD,
            ) as conn:
                from utils.columns import get_ods_columns
                ods_columns = get_ods_columns()
                with conn.cursor() as cur:
                    cur.execute('set search_path to stg;')
                    cur.copy_from(
                        file=buffer,
                        table='taxi_data',
                        columns=ods_columns,
                        sep='\t',
                        null=''
                    )
                    print(f'Loaded {len(df_valid)} rows into stg.taxi_data')

                    cur.execute('select count(*) from stg.taxi_data;')
                    print(f'stg.taxi_data count: {cur.fetchone()[0]}')

                conn.commit()
                print('Inserted into stg.taxi_data')

        engine = create_engine(
            f'postgresql+psycopg2://{POSTGRES_DWH_USER}:'
            f'{POSTGRES_DWH_PASSWORD}@'
            f'{POSTGRES_DWH_HOST}:'
            f'{POSTGRES_DWH_PORT}/'
            f'{POSTGRES_DWH_DB}'
        )
        print('Inserting into ods...')
        with engine.connect() as conn:
            df_valid.to_sql(
                name='taxi_data',
                schema='ods',
                con=conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=batch_size
            )

        print('Executed')
    except Exception as err:
        print(err)
        raise AirflowException(err)


# dag initialization
@dag(
    dag_id='d_process_data_into_ods',
    schedule=None,
    catchup=False,
    start_date=datetime(year=2024, month=11, day=6)
)
def d_process_data_into_ods():
    @task
    def d_get_covered_dates():
        return _d_get_covered_dates()

    @task
    def d_check_instance(date):
        return _d_check_instance(date)

    @task
    def d_process_data(instance_data):
        object_name, year = instance_data
        _d_process_data(object_name, year, 10_000)

    covered_dates = d_get_covered_dates()
    instance_data = d_check_instance(covered_dates)
    d_process_data(instance_data)


d_process_data_into_ods()
