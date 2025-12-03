import os
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


# migrations
def _run_migrations():
    init_schemas = '''
    create schema if not exists stg;
    create schema if not exists ods;
    create schema if not exists dm;
    
    comment on schema stg is 'Staging';
    comment on schema ods is 'Operational data store';
    comment on schema dm is 'Data marts';
    '''
    create_stg_tables = '''
    create table if not exists stg.weather_data (
	id_measure serial primary key,
	raw_path varchar(255) not null,
	load_time timestamp default current_timestamp,
	dt bigint not null,
	city varchar(100) not null,
	source varchar(100) default 'api.openweathermap.org'
	);
    '''

    # TODO create_ods_tables

    migrations = (init_schemas, create_stg_tables)

    with psycopg2.connect(
            host=os.getenv('POSTGRES_DWH_HOST'),
            port=os.getenv('POSTGRES_DWH_PORT'),
            dbname=os.getenv('POSTGRES_DWH_DB'),
            user=os.getenv('POSTGRES_DWH_USER'),
            password=os.getenv('POSTGRES_DWH_PASSWORD'),
        ) as conn:
        for script in migrations:
            try:
                with conn.cursor() as cur:
                    cur.execute(script)
            except Exception as e:
                print(e)
                raise ConnectionError
            conn.commit()
            print(f'executed')

# dag initialization
with DAG('sql_migrations', start_date=datetime(2024, 12, 1),
         schedule=None, catchup=False) as dag:
    run_migrations = PythonOperator(
        task_id='run_migrations',
        python_callable=_run_migrations
    )
