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


# getting dates from context
def _get_covered_dates():
    context = get_current_context()['dag']
    date = context.start_date
    return date


def _check_instance(date):
    with duckdb.connect(database=':memory') as con:
        con.execute(f'''attach
            'host={POSTGRES_DWH_HOST} 
            port={POSTGRES_DWH_PORT}
            dbname={POSTGRES_DWH_DB} 
            user={POSTGRES_DWH_USER} 
            password={POSTGRES_DWH_PASSWORD}'
            as reg(type postgres, schema reg)''')

        covered_date = f'{date.year}_{date.month:02d}'
        print(covered_date)
        result = con.execute(
            '''
            select raw_path 
            from reg.taxi_data
            where covered_dates = ?
                and processed = false
            limit 1
            ''', [covered_date]).fetchall()
        # TODO: Убрать LIMIT 1 и вызывать ошибку когда более 1 результата (сейчас для удобства разработки)

        if len(result) > 1:
            print(f'Warning: more than 1 non-processed records for \'{covered_date}\'')

        print(f'Executed\n')
        print(result)
        # [][] because returns tuple inside list
        return result[0][0], date.year


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
            df = df.head(20_000)

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

                con.begin()
                try:
                    print(f'Staging executed\n')
                    # load into ods
                    con.execute('''load postgres''')
                    con.execute(f'''
                        attach
                        'host={POSTGRES_DWH_HOST} 
                        port={POSTGRES_DWH_PORT}
                        dbname={POSTGRES_DWH_DB} 
                        user={POSTGRES_DWH_USER} 
                        password={POSTGRES_DWH_PASSWORD}'
                        as ods(type postgres, schema ods)
                    ''')

                    print('Successfully connected to an ods table')

                    offset = 0
                    while offset < total_records:
                        con.execute(f'''
                            insert into ods.taxi_data
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
                                tpep_pickup + interval '3 hours',
                                tpep_dropoff + interval '3 hours',
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
                    con.commit()

                    con.execute(f'''
                        attach
                        'host={POSTGRES_DWH_HOST} 
                        port={POSTGRES_DWH_PORT}
                        dbname={POSTGRES_DWH_DB} 
                        user={POSTGRES_DWH_USER} 
                        password={POSTGRES_DWH_PASSWORD}'
                        as reg(type postgres, schema reg)
                    ''')

                    print('Successfully connected to a reg table')

                    con.execute(f'''
                        update reg.taxi_data
                        set processed = true,
                            processed_time = current_localtimestamp()
                        where raw_path = ?
                    ''', [object_name])

                    print('Executed')
                except Exception as err:
                    con.rollback()
                    print(f' {err}')
                    raise AirflowException(err)

        print('Executed')
        return None
    except Exception as err:
        print(err)
        return None


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
    def get_covered_dates():
        return _get_covered_dates()

    @task
    def check_instance(covered_dates):
        return _check_instance(covered_dates)

    @task
    def process_data(instance_data):
        object_name, year = instance_data
        _process_data(object_name, year, 5_000)

    covered_dates = get_covered_dates()
    instance_data = check_instance(covered_dates)
    process_data(instance_data)


process_data_into_ods()
