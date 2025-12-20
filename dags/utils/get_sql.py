def get_duckdb_temp_table_sql(year: str, table_name: str):
    if int(year) >= 2025:
        sql = f'''
            create table {table_name} (
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
            )
            '''
    else:
        sql = f'''
            create table {table_name} (
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
                airport_fee varchar
            )
            '''
    return sql

def get_duckdb_temp_validate_table_sql(year: str, table_name: str):
    if int(year) >= 2025:
        sql = f'''
            create table {table_name} (
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
                cbd_congestion_fee numeric(12,2) default 0.0
            )
            '''
    else:
        sql = f'''
            create table {table_name} (
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
                airport_fee numeric(12,2) default 0.0
            )
            '''
    return sql

def get_temp_table_sql(year: str):
    if int(year) >= 2025:
        sql = '''
            create temp table staging (
                vendor_id text,
                tpep_pickup text,
                tpep_dropoff text,
                passenger_count text,
                trip_distance text,
                ratecode_id text,
                store_and_forward text,
                pu_location_id text,
                do_location_id text,
                payment_type text,
                fare text,
                extras text,
                mta_tax text,
                tip text,
                tolls text,
                improvement text,
                total text,
                congestion text,
                airport_fee text,
                cbd_congestion_fee text
            ) on commit drop;
            '''
    else:
        sql = '''
            create temp table staging (
                vendor_id text,
                tpep_pickup text,
                tpep_dropoff text,
                passenger_count text,
                trip_distance text,
                ratecode_id text,
                store_and_forward text,
                pu_location_id text,
                do_location_id text,
                payment_type text,
                fare text,
                extras text,
                mta_tax text,
                tip text,
                tolls text,
                improvement text,
                total text,
                congestion text,
                airport_fee text
            ) on commit drop;
            '''
    return sql