import os
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from airflow.sdk import dag, task, get_current_context


# migrations
def _run_migrations():
    init_schemas = '''
        create schema if not exists stg;
        create schema if not exists reg;
        create schema if not exists ods;
        create schema if not exists dm;
        
        comment on schema reg is 'Registry';
        comment on schema stg is 'Staging';
        comment on schema ods is 'Operational data store';
        comment on schema dm is 'Data marts';
    '''

    create_reg_tables = '''
        create table if not exists reg.taxi_data (
        id_measure serial primary key,
        raw_path varchar(255) not null,
        covered_dates varchar(50) not null,
        load_time timestamp default current_timestamp,
        source varchar(100) default 'https://d37ci6vzurychx.cloudfront.net',
        processed boolean default false,
        processed_time timestamp default null,
        file_size bigint not null
        );
    '''

    create_reg_indexes = '''
        create index if not exists idx_reg_сovered_dates on reg.taxi_data (covered_dates);
        '''

    create_stg_tables = '''
        create table if not exists stg.taxi_data (
        id serial primary key,
        vendor_id varchar,
        tpep_pickup varchar,
        tpep_dropoff varchar,
        passenger_count varchar,
        trip_distance varchar,
        ratecode_id varchar,
        store_and_forward varchar,
        pu_location_id varchar,
        do_location_id varchar,
        payment_type varchar,
        fare varchar,
        extras varchar,
        mta_tax varchar,
        tip varchar,
        tolls varchar,
        improvement varchar,
        total varchar,
        congestion varchar,
        airport_fee varchar,
        cbd_congestion_fee varchar
        );
    '''

    create_ods_tables = '''
        create table if not exists ods.vendor(
            id smallint primary key,
            description varchar(50) not null
            );
        
        create table if not exists ods.ratecode(
            id smallint primary key,
            description varchar(50) not null
            );
        
        create table if not exists ods.payment(
            id smallint primary key,
            description varchar(50) not null
            );
        
        create table if not exists ods.taxi_data (
            id serial primary key,
            vendor_id smallint not null,
            tpep_pickup timestamp not null,
            tpep_dropoff timestamp not null,
            passenger_count smallint not null,
            trip_distance numeric(12,2) not null,
            ratecode_id smallint default 99,
            store_and_forward boolean not null,
            pu_location_id smallint,
            do_location_id smallint,
            payment_type smallint not null,
            fare numeric(12,2) not null,
            extras numeric(12,2) not null,
            mta_tax numeric(12,2) not null,
            tip numeric(12,2) not null,
            tolls numeric(12,2) not null,
            improvement numeric(12,2) not null,
            total numeric(12,2) not null,
            congestion numeric(12,2) not null,
            airport_fee numeric(12,2) default 0.0,
            cbd_congestion_fee numeric(12,2) default 0.0,
            source_system varchar(50) default 'TLC Taxi'
            );
        
        create table if not exists ods.taxi_data_quarantine (
            id serial primary key,
            vendor_id smallint not null,
            tpep_pickup timestamp not null,
            tpep_dropoff timestamp not null,
            passenger_count smallint not null,
            trip_distance numeric(12,2) not null,
            ratecode_id smallint default 99,
            store_and_forward boolean not null,
            pu_location_id smallint,
            do_location_id smallint,
            payment_type smallint not null,
            fare numeric(12,2) not null,
            extras numeric(12,2) not null,
            mta_tax numeric(12,2) not null,
            tip numeric(12,2) not null,
            tolls numeric(12,2) not null,
            improvement numeric(12,2) not null,
            total numeric(12,2) not null,
            congestion numeric(12,2) not null,
            airport_fee numeric(12,2) default 0.0,
            cbd_congestion_fee numeric(12,2) default 0.0,
            source_system varchar(50) default 'TLC Taxi'
            );
    '''

    insert_reference_data = '''
        insert into ods.vendor(id, description)
            values (1, 'Creative Mobile Technologies, LLC'),
                (2, 'Curb Mobility, LLC'),
                (6, 'Myle Technologies Inc'),
                (7, 'Helix')
            on conflict (id) do nothing;
        
        insert into ods.ratecode(id, description)
            values (1, 'Standard rate'),
                (2, 'JFK'),
                (3, 'Newark'),
                (4, 'Nassau or Westchester'),
                (5, 'Negotiated fare'),
                (6, 'Group ride'),
                (99, 'Null/unknown')
            on conflict (id) do nothing;
        
        insert into ods.payment(id, description)
            values (0, 'Flex Fare trip'),
                (1, 'Credit card'),
                (2, 'Cash'),
                (3, 'No charge'),
                (4, 'Dispute'),
                (5, 'Unknown'),
                (6, 'Voided trip')
            on conflict (id) do nothing;
    '''

    add_constraints = '''
        alter table ods.taxi_data
        add constraint fk__taxi__data_vendor_id__vendor__id foreign key (vendor_id) references ods.vendor(id)
                on delete no action,
        add constraint fk__taxi__ratecode_id__ratecode__id foreign key (ratecode_id) references ods.ratecode(id)
                on delete no action,
        add constraint fk__taxi__payment_type__payment__id foreign key (payment_type) references ods.payment(id)
                on delete no action;
    '''

    create_dm_tables = '''
        create table if not exists dm.daily_metrics(
            date timestamp primary key,
            avg_total numeric(12, 2) not null,
            avg_tip numeric(12, 2) not null,
            avg_passenger_count numeric(12, 2) not null,
            avg_trip_distance numeric(12,2) not null
            );
    '''

    migrations = (init_schemas, create_reg_tables, create_reg_indexes, create_stg_tables, create_ods_tables,
                  insert_reference_data, create_dm_tables)

    with psycopg2.connect(
            host=os.getenv('POSTGRES_DWH_HOST'),
            port=os.getenv('POSTGRES_DWH_PORT'),
            dbname=os.getenv('POSTGRES_DWH_DB'),
            user=os.getenv('POSTGRES_DWH_USER'),
            password=os.getenv('POSTGRES_DWH_PASSWORD'),
    ) as conn:
        with conn.cursor() as cur:
            for script in migrations:
                try:
                    cur.execute(script)
                except Exception as e:
                    print(e)
                    raise
            try:
                cur.execute(add_constraints)
            except psycopg2.errors.DuplicateObject:
                pass
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
