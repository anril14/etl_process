import os
import psycopg2
from airflow import DAG
from datetime import datetime
from airflow.sdk import dag, task, get_current_context
from utils.get_env import (
    POSTGRES_DWH_HOST,
    POSTGRES_DWH_PORT,
    POSTGRES_DWH_DB,
    POSTGRES_DWH_USER,
    POSTGRES_DWH_PASSWORD
)


# migrations
def _run_migrations():
    init_schemas = '''
        CREATE SCHEMA IF NOT EXISTS stg;
        CREATE SCHEMA IF NOT EXISTS reg;
        CREATE SCHEMA IF NOT EXISTS ods;
        CREATE SCHEMA IF NOT EXISTS dm;
        
        COMMENT ON SCHEMA reg IS 'Registry';
        COMMENT ON SCHEMA stg IS 'Staging';
        COMMENT ON SCHEMA ods IS 'Operational data store';
        COMMENT ON SCHEMA dm IS 'Data marts';
    '''

    create_reg_tables = '''
        CREATE TABLE IF NOT EXISTS reg.taxi_data (
        id_measure serial PRIMARY KEY,
        raw_path VARCHAR(255) NOT NULL,
        covered_dates VARCHAR(50) NOT NULL,
        load_time TIMESTAMP DEFAULT current_TIMESTAMP,
        source VARCHAR(100) DEFAULT 'https://d37ci6vzurychx.cloudfront.net',
        processed boolean DEFAULT FALSE,
        processed_time TIMESTAMP DEFAULT NULL,
        file_size bigint NOT NULL
        );
    '''

    create_reg_indexes = '''
        create index if not exists idx_reg_сovered_dates on reg.taxi_data (covered_dates);
        '''

    create_stg_tables = '''
        CREATE TABLE IF NOT EXISTS stg.taxi_data (
        id serial PRIMARY KEY,
        vendor_id VARCHAR,
        tpep_pickup VARCHAR,
        tpep_dropoff VARCHAR,
        passenger_count VARCHAR,
        trip_distance VARCHAR,
        ratecode_id VARCHAR,
        store_and_forward VARCHAR,
        pu_location_id VARCHAR,
        do_location_id VARCHAR,
        payment_type VARCHAR,
        fare VARCHAR,
        extras VARCHAR,
        mta_tax VARCHAR,
        tip VARCHAR,
        tolls VARCHAR,
        improvement VARCHAR,
        total VARCHAR,
        congestion VARCHAR,
        airport_fee VARCHAR,
        cbd_congestion_fee VARCHAR
        );
    '''

    create_ods_tables = '''
        CREATE TABLE IF NOT EXISTS ods.vendor(
            id SMALLINT PRIMARY KEY,
            description VARCHAR(50) NOT NULL
            );
        
        CREATE TABLE IF NOT EXISTS ods.ratecode(
            id SMALLINT PRIMARY KEY,
            description VARCHAR(50) NOT NULL
            );
        
        CREATE TABLE IF NOT EXISTS ods.payment(
            id SMALLINT PRIMARY KEY,
            description VARCHAR(50) NOT NULL
            );
        
        CREATE TABLE IF NOT EXISTS ods.taxi_data (
            id serial PRIMARY KEY,
            vendor_id SMALLINT NOT NULL,
            tpep_pickup TIMESTAMP NOT NULL,
            tpep_dropoff TIMESTAMP NOT NULL,
            passenger_count SMALLINT NOT NULL,
            trip_distance NUMERIC(12,2) NOT NULL,
            ratecode_id SMALLINT DEFAULT 99,
            store_and_forward boolean NOT NULL,
            pu_location_id SMALLINT,
            do_location_id SMALLINT,
            payment_type SMALLINT NOT NULL,
            fare NUMERIC(12,2) NOT NULL,
            extras NUMERIC(12,2) NOT NULL,
            mta_tax NUMERIC(12,2) NOT NULL,
            tip NUMERIC(12,2) NOT NULL,
            tolls NUMERIC(12,2) NOT NULL,
            improvement NUMERIC(12,2) NOT NULL,
            total NUMERIC(12,2) NOT NULL,
            congestion NUMERIC(12,2) NOT NULL,
            airport_fee numeric(12,2) DEFAULT 0.0,
            cbd_congestion_fee numeric(12,2) DEFAULT 0.0,
            source_system VARCHAR(50) DEFAULT 'TLC Taxi'
            );
        
        CREATE TABLE IF NOT EXISTS ods.taxi_data_quarantine (
            id SERIAL PRIMARY KEY,
            vendor_id SMALLINT NOT NULL,
            tpep_pickup TIMESTAMP NOT NULL,
            tpep_dropoff TIMESTAMP NOT NULL,
            passenger_count SMALLINT NOT NULL,
            trip_distance NUMERIC(12,2) NOT NULL,
            ratecode_id SMALLINT DEFAULT 99,
            store_and_forward boolean NOT NULL,
            pu_location_id SMALLINT,
            do_location_id SMALLINT,
            payment_type SMALLINT NOT NULL,
            fare NUMERIC(12,2) NOT NULL,
            extras NUMERIC(12,2) NOT NULL,
            mta_tax NUMERIC(12,2) NOT NULL,
            tip NUMERIC(12,2) NOT NULL,
            tolls NUMERIC(12,2) NOT NULL,
            improvement NUMERIC(12,2) NOT NULL,
            total NUMERIC(12,2) NOT NULL,
            congestion NUMERIC(12,2) NOT NULL,
            airport_fee NUMERIC(12,2) DEFAULT 0.0,
            cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0,
            source_system VARCHAR(50) DEFAULT 'TLC Taxi'
            );
    '''

    insert_reference_data = '''
        INSERT INTO ods.vendor(id, description)
            VALUES (1, 'Creative Mobile Technologies, LLC'),
                (2, 'Curb Mobility, LLC'),
                (6, 'Myle Technologies Inc'),
                (7, 'Helix')
            ON CONFLICT (id) DO NOTHING;
        
        INSERT INTO ods.ratecode(id, description)
            VALUES (1, 'Standard rate'),
                (2, 'JFK'),
                (3, 'Newark'),
                (4, 'Nassau or Westchester'),
                (5, 'Negotiated fare'),
                (6, 'Group ride'),
                (99, 'Null/unknown')
            ON CONFLICT (id) DO NOTHING;
        
        INSERT INTO ods.payment(id, description)
            VALUES (0, 'Flex Fare trip'),
                (1, 'Credit card'),
                (2, 'Cash'),
                (3, 'No charge'),
                (4, 'Dispute'),
                (5, 'Unknown'),
                (6, 'Voided trip')
            ON CONFLICT (id) DO NOTHING;
    '''

    add_constraints = '''
        ALTER TABLE ods.taxi_data
        ADD CONSTRAINT fk__taxi__data_vendor_id__vendor__id FOREIGN KEY (vendor_id) REFERENCES ods.vendor(id)
                ON DELETE NO ACTION,
        ADD CONSTRAINT fk__taxi__ratecode_id__ratecode__id FOREIGN KEY (ratecode_id) REFERENCES ods.ratecode(id)
                ON DELETE NO ACTION,
        ADD CONSTRAINT fk__taxi__payment_type__payment__id FOREIGN KEY (payment_type) REFERENCES ods.payment(id)
                ON DELETE NO ACTION;
    '''

    create_dm_tables = '''
        CREATE TABLE IF NOT EXISTS dm.daily_metrics(
            date TIMESTAMP PRIMARY KEY,
            avg_total NUMERIC(12, 2) NOT NULL,
            avg_tip NUMERIC(12, 2) NOT NULL,
            avg_passenger_count NUMERIC(12, 2) NOT NULL,
            avg_trip_distance NUMERIC(12,2) NOT NULL
            );
    '''

    migrations = (init_schemas, create_reg_tables, create_reg_indexes, create_stg_tables, create_ods_tables,
                  insert_reference_data, create_dm_tables)

    with psycopg2.connect(
            host=POSTGRES_DWH_HOST,
            port=POSTGRES_DWH_PORT,
            dbname=POSTGRES_DWH_DB,
            user=POSTGRES_DWH_USER,
            password=POSTGRES_DWH_PASSWORD,
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
            except psycopg2.errors.DuplicateObject as err:
                print(f'Schemas or tables already created', err)
            conn.commit()
            print(f'Executed')


# dag initialization
@dag(dag_id='sql_migrations_taxi_data', start_date=datetime(2024, 12, 1),
     schedule=None, catchup=False)
def sql_migrations():
    @task
    def run_migrations():
        _run_migrations()

    run_migrations()


sql_migrations()
