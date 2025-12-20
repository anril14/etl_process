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
from utils.get_env import *
from utils.get_sql import *


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


def _process_data(object_name, year, batch_size):
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
            print(f'Bytes length: {len(response.data)}')

            df = pd.read_parquet(io.BytesIO(response.data))
            # TODO Small dataset
            df = df.head(50_000)

            print(f'Original column names:{df.columns}')
            from utils.columns import ODS_COLUMN_MAPPING
            df = df.rename(columns=ODS_COLUMN_MAPPING)
            print(f'Refactored column names:{df.columns}')

            # validate
            with duckdb.connect(database=':memory') as con:
                init_staging_sql = get_duckdb_temp_table_sql(year, 'staging')
                con.execute("drop table if exists staging")
                con.execute(init_staging_sql)

                con.register('df_view', df)
                con.execute(f'''insert into staging select * from df_view''')

                con.execute('''select count(*) from staging''')
                # print count
                print(f'Count of records: {con.fetchall()[0][0]}')

                init_staging_validate_sql = get_duckdb_validate_table_sql('staging_validate')
                con.execute("drop table if exists staging_validate")
                con.execute(init_staging_validate_sql)

                insert_sql = get_duckdb_insert_validate_sql(year, 'staging_validate')
                con.execute(insert_sql)
                con.execute('''select count(*) from staging_validate''')
                total_records = con.fetchall()[0][0]
                print(f'Count of validated records: {total_records}')

                print(f'Staging executed\n')
                # load into ods
                con.execute('''load postgres''')
                con.execute(f'''attach
                            'host={POSTGRES_DWH_HOST} 
                            port={POSTGRES_DWH_PORT}
                            dbname={POSTGRES_DWH_DB} 
                            user={POSTGRES_DWH_USER} 
                            password={POSTGRES_DWH_PASSWORD}'
                            as db(type postgres, schema ods)''')

                offset = 0
                while offset < total_records:
                    con.execute(f'''insert into db.taxi_data
                    (
                        vendor_id,
                        tpep_pickup,
                        tpep_dropoff,
                        passenger_count,
                        trip_distance,
                        ratecode_id,
                        store_and_forward,
                        pu_location_id,
                        do_location_id,
                        payment_type,
                        fare,
                        extras,
                        mta_tax,
                        tip,
                        tolls,
                        improvement,
                        total,
                        congestion,
                        airport_fee,
                        cbd_congestion_fee
                    )
                    select vendor_id,
                        tpep_pickup,
                        tpep_dropoff,
                        passenger_count,
                        trip_distance,
                        ratecode_id,
                        case 
                            when store_and_forward in ('N', 'n') then false 
                            else true 
                        end as store_and_forward,
                        pu_location_id,
                        do_location_id,
                        payment_type,
                        fare,
                        extras,
                        mta_tax,
                        tip,
                        tolls,
                        improvement,
                        total,
                        congestion,
                        airport_fee,
                        cbd_congestion_fee
                    from staging_validate
                    where 
                        vendor_id in (1, 2, 6, 7) and
                        ratecode_id in (1, 2, 3, 4, 5, 6, 99) and
                        payment_type in (0, 1, 2, 3, 4, 5, 6)
                    limit {batch_size}
                    offset {offset}
                    ''')
                    print(f'Loaded {offset + batch_size} total records')
                    offset += batch_size

        print('Executed')
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
        _process_data(object_name, year, 5_000)

    instance_data = check_instance()
    process_data(instance_data)


process_data_into_ods()
