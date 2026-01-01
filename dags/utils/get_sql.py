from datetime import datetime


def get_duckdb_create_temp_table_sql(year, table_name: str):
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


def get_duckdb_create_validate_table_sql(table_name: str):
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
    return sql


def get_duckdb_insert_validate_sql(year, table_from, table_to: str):
    if int(year) >= 2025:
        sql = f'''insert into {table_to}
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
        select * 
        from {table_from}
        where
            try_cast(vendor_id as smallint) is not null and
            try_cast(tpep_pickup as timestamp) is not null and
            try_cast(tpep_dropoff as timestamp) is not null and
            try_cast(passenger_count as smallint) is not null and
            try_cast(trip_distance as numeric(12,2)) is not null and
            try_cast(ratecode_id as smallint) is not null and
            store_and_forward in ('N', 'Y', 'n', 'y') and
            try_cast(pu_location_id as smallint) is not null and
            try_cast(do_location_id as smallint) is not null and
            try_cast(payment_type as smallint) is not null and
            try_cast(fare as numeric(12,2)) is not null and
            try_cast(extras as numeric(12,2)) is not null and
            try_cast(mta_tax as numeric(12,2)) is not null and
            try_cast(tip as numeric(12,2)) is not null and
            try_cast(tolls as numeric(12,2)) is not null and
            try_cast(improvement as numeric(12,2)) is not null and
            try_cast(total as numeric(12,2)) is not null and
            try_cast(congestion as numeric(12,2)) is not null and
            try_cast(airport_fee as numeric(12,2)) is not null and
            try_cast(cbd_congestion_fee as numeric(12,2)) is not null
        '''
    else:
        sql = f'''insert into {table_to}
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
        airport_fee
        )
        select * 
        from {table_from}
        where
            try_cast(vendor_id as smallint) is not null and
            try_cast(tpep_pickup as timestamp) is not null and
            try_cast(tpep_dropoff as timestamp) is not null and
            try_cast(passenger_count as smallint) is not null and
            try_cast(trip_distance as numeric(12,2)) is not null and
            try_cast(ratecode_id as smallint) is not null and
            store_and_forward in ('N', 'Y', 'n', 'y') and
            try_cast(pu_location_id as smallint) is not null and
            try_cast(do_location_id as smallint) is not null and
            try_cast(payment_type as smallint) is not null and
            try_cast(fare as numeric(12,2)) is not null and
            try_cast(extras as numeric(12,2)) is not null and
            try_cast(mta_tax as numeric(12,2)) is not null and
            try_cast(tip as numeric(12,2)) is not null and
            try_cast(tolls as numeric(12,2)) is not null and
            try_cast(improvement as numeric(12,2)) is not null and
            try_cast(total as numeric(12,2)) is not null and
            try_cast(congestion as numeric(12,2)) is not null and
            try_cast(airport_fee as numeric(12,2)) is not null
        '''
    return sql


def get_duckdb_create_valid_tables_sql(table_name1, table_name2: str):
    sql1 = f'''
    create table {table_name1} (
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

    sql2 = f'''
    create table {table_name2} (
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
    return sql1, sql2


def get_duckdb_insert_valid_data(covered_dates: datetime, table_to_valid, table_to_invalid, table_from: str):
    sql1 = f'''
    insert into {table_to_valid}
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
        tpep_pickup + interval '3 hours' as tpep_pickup,
        tpep_dropoff + interval '3 hours' as tpep_dropoff,
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
    from {table_from}
    where
        year(tpep_pickup + interval '3 hours') = {int(covered_dates.year)} and
        month(tpep_pickup + interval '3 hours') = {int(covered_dates.month)} and
        vendor_id in (1, 2, 6, 7) and
        ratecode_id in (1, 2, 3, 4, 5, 6, 99) and
        payment_type in (0, 1, 2, 3, 4, 5, 6)
    '''

    sql2 = f'''
    insert into {table_to_invalid}
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
        tpep_pickup + interval '3 hours' as tpep_pickup,
        tpep_dropoff + interval '3 hours' as tpep_dropoff,
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
    from {table_from}
    where not(
        year(tpep_pickup + interval '3 hours') = {int(covered_dates.year)} and
        month(tpep_pickup + interval '3 hours') = {int(covered_dates.month)} and
        vendor_id in (1, 2, 6, 7) and
        ratecode_id in (1, 2, 3, 4, 5, 6, 99) and
        payment_type in (0, 1, 2, 3, 4, 5, 6)
    )
    '''
    return sql1, sql2


# deprecated
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
