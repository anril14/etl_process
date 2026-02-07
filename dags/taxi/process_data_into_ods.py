import io
import json
import os

import duckdb
import psycopg2
import pyarrow.parquet
from dotenv import dotenv_values
from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task, get_current_context, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from minio import Minio
from utils.get_sql import (
    get_duckdb_create_temp_table_sql,
    get_duckdb_create_validate_table_sql,
    get_duckdb_create_valid_tables_sql,
    get_duckdb_insert_valid_data,
    get_duckdb_insert_validate_sql,
)
from utils.connections import get_duckdb_connection
from utils.get_env import (
    MINIO_BUCKET_NAME,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


# getting dates from context
def _get_covered_dates():
    context = get_current_context()
    conf = context['dag_run'].conf
    if conf and conf['year'] and conf['month']:
        print(f'conf {conf}')

        # TODO Pydantic configuration check
        try:
            year = int(conf['year'])
            month = int(conf['month'])
        except Exception:
            raise ValueError('Wrong datetime configuration format')

        date = datetime(year, month, 1)
        return date
    else:
        raise ValueError('Invalid date format from previous dag')


def _check_instance(date):
    print(f'result date: {date}')
    with get_duckdb_connection(db_schemas=['reg']) as con:

        covered_date = f'{date.year}_{date.month:02d}'
        print(f'covered_date: {covered_date}')
        result = con.execute(
            '''
            SELECT raw_path 
            FROM reg.taxi_data
            WHERE covered_dates = ?
                AND processed = FALSE
            ''', [covered_date]).fetchall()

        if len(result) > 1:
            logging.warning(f'More than 1 non-processed records for \'{covered_date}\'')

        print(f'Executed\n')
        print(f'result: {result}')
        # [][] because returns tuple inside list
        if len(result) == 0:
            raise ValueError('No right values in registry')

        return result[0][0]


def _process_data(object_name, date, batch_size):
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
            # Smaller dataset
            df = df.head(100_000)

            print(f'Original column names:{df.columns}')
            from utils.columns import ODS_COLUMN_MAPPING
            df = df.rename(columns=ODS_COLUMN_MAPPING)
            print(f'Refactored column names:{df.columns}')

            # validate
            with get_duckdb_connection(db_schemas=['reg', 'ods']) as con:

                init_staging_sql = get_duckdb_create_temp_table_sql(str(date.year), 'staging')
                con.execute("DROP TABLE IF EXISTS staging")
                con.execute(init_staging_sql)

                con.register('df_view', df)
                con.execute(f'''INSERT INTO staging SELECT * FROM df_view''')

                con.execute('''SELECT COUNT(*) FROM staging''')
                # print count
                print(f'Count of records: {con.fetchall()[0][0]}')

                init_staging_validate_sql = get_duckdb_create_validate_table_sql('staging_validate')
                con.execute("DROP TABLE IF EXISTS staging_validate")
                con.execute(init_staging_validate_sql)

                insert_validate_sql = get_duckdb_insert_validate_sql(str(date.year), 'staging', 'staging_validate')

                con.execute(insert_validate_sql)

                con.execute('''SELECT COUNT(*) FROM staging_validate''')
                # print count
                print(f'Count of valid records: {con.fetchall()[0][0]}')

                init_valid_sql, init_invalid_sql = get_duckdb_create_valid_tables_sql('staging_valid',
                                                                                      'staging_invalid')
                con.execute("DROP TABLE IF EXISTS staging_valid")
                con.execute("DROP TABLE IF EXISTS staging_invalid")
                con.execute(init_valid_sql)
                con.execute(init_invalid_sql)

                insert_valid_sql, insert_invalid_sql = get_duckdb_insert_valid_data(date, 'staging_valid',
                                                                                    'staging_invalid',
                                                                                    'staging_validate')
                con.execute(insert_valid_sql)
                con.execute(insert_invalid_sql)

                con.execute('''SELECT COUNT(*) FROM staging_valid''')
                total_completed = con.fetchall()[0][0]
                print(f'Count of complete records: {total_completed}')
                con.execute('''SELECT COUNT(*) FROM staging_invalid''')
                total_quarantine = con.fetchall()[0][0]
                print(f'Count of quarantine records: {total_quarantine}')

                con.begin()
                try:
                    offset = 0
                    while offset < total_completed:
                        con.execute(f'''
                            INSERT INTO ods.taxi_data
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
                            SELECT vendor_id,
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
                            FROM staging_valid
                            LIMIT {batch_size}
                            OFFSET {offset}
                        ''')
                        print(f'Loaded {offset + batch_size} total records into completed table')
                        offset += batch_size
                    offset = 0
                    while offset < total_quarantine:
                        con.execute(f'''
                            INSERT INTO ods.taxi_data_quarantine
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
                            SELECT vendor_id,
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
                            FROM staging_invalid
                            LIMIT {batch_size}
                            OFFSET {offset}
                        ''')
                        print(f'Loaded {offset + batch_size} total records into quarantine table')
                        offset += batch_size
                    con.commit()

                    con.execute(f'''
                        UPDATE reg.taxi_data
                        SET processed = true,
                            processed_time = CURRENT_LOCALTIMESTAMP()
                        WHERE raw_path = ?
                    ''', [object_name])

                    print('Insertion executed')
                except Exception as err:
                    con.rollback()
                    print(err)
                    raise AirflowException(err)

        print('Executed')
        return None
    except Exception as err:
        print(err)
        raise AirflowException(err)


# dag initialization
@dag(
    dag_id='process_data_into_ods',
    schedule=None,
    catchup=False
)
def process_data_into_ods():
    @task
    def get_covered_dates():
        return _get_covered_dates()

    @task
    def check_instance(date):
        return _check_instance(date)

    @task
    def process_data(instance_data):
        object_name, covered_dates = instance_data
        # TODO Custom batch size
        _process_data(object_name, covered_dates, 10_000)

    trigger_recalculate = TriggerDagRunOperator(
        task_id="trigger_recalculate",
        trigger_dag_id="recalculate_data_mart",
        wait_for_completion=False,
    )

    covered_dates = get_covered_dates()
    instance_data = check_instance(covered_dates), covered_dates
    processed = process_data(instance_data)

    processed >> trigger_recalculate


process_data_into_ods()
