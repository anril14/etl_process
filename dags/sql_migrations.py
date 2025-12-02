import os
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


# migrations
def _run_migrations():
    migrations = [
        '001_init_schemas.sql',
        '002_create_stg_tables.sql',
        '003_create_ods_tables.sql'
    ]
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_DWH_HOST'),
            port=os.getenv('POSTGRES_DWH_PORT'),
            dbname=os.getenv('POSTGRES_DWH_DB'),
            user=os.getenv('POSTGRES_DWH_USER'),
            password=os.getenv('POSTGRES_DWH_PASSWORD'),
        )

        # TODO Донастроить миграции через DAG
        sql_dir = os.path.abspath('../sql/')
        for file_name in migrations:
            with open(os.path.join(sql_dir, file_name), 'r') as f:
                sql = f.read()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
            except Exception as e:
                print(e)
                raise ConnectionError

            conn.commit()
            print(f'executed')
    except Exception as e:
        conn.rollback()
        print(e)
        raise ConnectionError
    finally:
        if conn:
            conn.close()


# dag initialization
with DAG('sql_migrations', start_date=datetime(2024, 12, 1),
         schedule=None, catchup=False) as dag:
    run_migrations = PythonOperator(
        task_id='run_migrations',
        python_callable=_run_migrations
    )
