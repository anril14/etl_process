import os
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from airflow.sdk import dag, task, get_current_context

# TODO: Refactor migrations to actual state

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
    create table if not exists stg.taxi_data (
	id_measure serial primary key,
	raw_path varchar(255) not null,
	covered_dates varchar(50) not null,
	load_time timestamp default current_timestamp,
	source varchar(100) default 'https://d37ci6vzurychx.cloudfront.net',
	processed boolean default false,
	processed_time timestamp default null,
	file_size bigint not null
	);
	
    create index if not exists idx_stg_сovered_dates on stg.taxi_data (covered_dates);
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
@dag(dag_id='sql_migrations', start_date=datetime(2024, 12, 1),
     schedule=None, catchup=False)
def sql_migrations():
    @task
    def run_migrations():
        _run_migrations()

    run_migrations()


sql_migrations()
