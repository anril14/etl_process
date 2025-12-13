import io
import json
import os
import psycopg2
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
                    from reg.taxi_data
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
            print(result)
            # [][] because returns tuple inside list
            return result[0][0], date.year
    except psycopg2.Error as err:
        print(f'psycopg2 error: {err}')
        raise TypeError(err)


def _process_data(object_name, year):
    import pandas as pd
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

            df = pd.read_parquet(io.BytesIO(response.data))
            # df = df.head(1000)
            print(df.head())

            from utils.columns import ODS_COLUMN_MAPPING
            df = df.rename(columns=ODS_COLUMN_MAPPING)

            print(f'Starts buffering...')
            buffer = io.StringIO()
            df.to_csv(buffer,
                      sep='\t',
                      header=False,
                      index=False,
                      na_rep='\\N'
                      )
            buffer.seek(0)
            print(f'Finished buffering')

            with psycopg2.connect(
                    host=POSTGRES_DWH_HOST,
                    port=POSTGRES_DWH_PORT,
                    dbname=POSTGRES_DWH_DB,
                    user=POSTGRES_DWH_USER,
                    password=POSTGRES_DWH_PASSWORD,
            ) as conn:
                from utils.sql import get_temp_table_sql
                sql = get_temp_table_sql(year)
                print(sql)
                with conn.cursor() as cur:
                    cur.execute(sql)

                    from utils.columns import get_ods_columns
                    ods_columns = get_ods_columns(year)
                    print(f'Created temp table')
                    cur.copy_from(file=buffer,
                                  table='staging',
                                  columns=ods_columns,
                                  sep='\t',
                                  null='')
                    print(f'Loaded {len(df)} into staging')

                    cur.execute(
                        '''
                        select count(*) from staging;
                        '''
                    )
                    print(f'{cur.fetchone()[0]}')

                conn.commit()
                print(f'Executed\n')


    except Exception as err:
        print(err)
        raise AirflowException(err)


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
    def process_data(instance_data):
        object_name, year = instance_data
        _process_data(object_name, year)

    instance_data = check_instance()
    process_data(instance_data)


process_data_into_ods()
