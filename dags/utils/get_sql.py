from datetime import datetime


def get_duckdb_create_temp_table_sql(year, table_name: str):
    if int(year) >= 2025:
        sql = f'''
            CREATE TABLE {table_name} (
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
            )
            '''
    else:
        sql = f'''
            CREATE TABLE {table_name} (
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
                airport_fee VARCHAR
            )
            '''
    return sql


def get_duckdb_create_validate_table_sql(table_name: str):
    sql = f'''
        CREATE TABLE {table_name} (
            vendor_id SMALLINT NOT NULL,
            tpep_pickup TIMESTAMP NOT NULL,
            tpep_dropoff TIMESTAMP NOT NULL,
            passenger_count SMALLINT NOT NULL,
            trip_distance NUMERIC(12,2) NOT NULL,
            ratecode_id SMALLINT DEFAULT 99,
            store_and_forward BOOLEAN NOT NULL,
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
            cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0
        )
        '''
    return sql


def get_duckdb_insert_validate_sql(year, table_from, table_to: str):
    if int(year) >= 2025:
        sql = f'''INSERT INTO {table_to}
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
        SELECT * 
        FROM {table_from}
        WHERE
            TRY_CAST(vendor_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(tpep_pickup AS TIMESTAMP) IS NOT NULL AND
            TRY_CAST(tpep_dropoff AS TIMESTAMP) IS NOT NULL AND
            TRY_CAST(passenger_count AS SMALLINT) IS NOT NULL AND
            TRY_CAST(trip_distance AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(ratecode_id AS SMALLINT) IS NOT NULL AND
            store_and_forward IN ('N', 'Y', 'n', 'y') AND
            TRY_CAST(pu_location_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(do_location_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(payment_type AS SMALLINT) IS NOT NULL AND
            TRY_CAST(fare AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(extras AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(mta_tax AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(tip AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(tolls AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(improvement AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(total AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(congestion AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(airport_fee AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(cbd_congestion_fee AS NUMERIC(12,2)) is NOT NULL
        '''
    else:
        sql = f'''INSERT INTO {table_to}
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
        SELECT * 
        FROM {table_from}
        WHERE
            TRY_CAST(vendor_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(tpep_pickup AS TIMESTAMP) IS NOT NULL AND
            TRY_CAST(tpep_dropoff AS TIMESTAMP) IS NOT NULL AND
            TRY_CAST(passenger_count AS SMALLINT) IS NOT NULL AND
            TRY_CAST(trip_distance AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(ratecode_id AS SMALLINT) IS NOT NULL AND
            store_and_forward IN ('N', 'Y', 'n', 'y') AND
            TRY_CAST(pu_location_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(do_location_id AS SMALLINT) IS NOT NULL AND
            TRY_CAST(payment_type AS SMALLINT) IS NOT NULL AND
            TRY_CAST(fare AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(extras AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(mta_tax AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(tip AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(tolls AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(improvement AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(total AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(congestion AS NUMERIC(12,2)) IS NOT NULL AND
            TRY_CAST(airport_fee AS NUMERIC(12,2)) IS NOT NULL
        '''
    return sql


def get_duckdb_create_valid_tables_sql(table_name1, table_name2: str):
    sql1 = f'''
    CREATE TABLE {table_name1} (
        vendor_id SMALLINT NOT NULL,
        tpep_pickup TIMESTAMP NOT NULL,
        tpep_dropoff TIMESTAMP NOT NULL,
        passenger_count SMALLINT NOT NULL,
        trip_distance NUMERIC(12,2) NOT NULL,
        ratecode_id SMALLINT DEFAULT 99,
        store_and_forward BOOLEAN NOT NULL,
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
        cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0
    )
    '''

    sql2 = f'''
    CREATE TABLE {table_name2} (
       vendor_id SMALLINT NOT NULL,
       tpep_pickup TIMESTAMP NOT NULL,
       tpep_dropoff TIMESTAMP NOT NULL,
       passenger_count SMALLINT NOT NULL,
       trip_distance NUMERIC(12,2) NOT NULL,
       ratecode_id SMALLINT DEFAULT 99,
       store_and_forward BOOLEAN NOT NULL,
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
       cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0
    )
    '''
    return sql1, sql2


def get_duckdb_insert_valid_data(covered_dates: datetime, table_to_valid, table_to_invalid, table_from: str):
    sql1 = f'''
    INSERT INTO {table_to_valid}
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
        tpep_pickup + INTERVAL '3 hours' AS tpep_pickup,
        tpep_dropoff + INTERVAL '3 hours' AS tpep_dropoff,
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
    FROM {table_from}
    WHERE
        year(tpep_pickup + INTERVAL '3 hours') = {int(covered_dates.year)} AND
        month(tpep_pickup + INTERVAL '3 hours') = {int(covered_dates.month)} AND
        vendor_id in (1, 2, 6, 7) AND
        ratecode_id in (1, 2, 3, 4, 5, 6, 99) AND
        payment_type in (0, 1, 2, 3, 4, 5, 6)
    '''

    sql2 = f'''
    INSERT INTO {table_to_invalid}
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
        tpep_pickup + INTERVAL '3 hours' AS tpep_pickup,
        tpep_dropoff + INTERVAL '3 hours' AS tpep_dropoff,
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
    FROM {table_from}
    WHERE NOT(
        year(tpep_pickup + INTERVAL '3 hours') = {int(covered_dates.year)} AND
        month(tpep_pickup + INTERVAL '3 hours') = {int(covered_dates.month)} AND
        vendor_id in (1, 2, 6, 7) AND
        ratecode_id in (1, 2, 3, 4, 5, 6, 99) AND
        payment_type in (0, 1, 2, 3, 4, 5, 6)
    )
    '''
    return sql1, sql2


# deprecated
def get_temp_table_sql(year: str):
    if int(year) >= 2025:
        sql = '''
            CREATE TEMP TABLE staging (
                vendor_id TEXT,
                tpep_pickup TEXT,
                tpep_dropoff TEXT,
                passenger_count TEXT,
                trip_distance TEXT,
                ratecode_id TEXT,
                store_and_forward TEXT,
                pu_location_id TEXT,
                do_location_id TEXT,
                payment_type TEXT,
                fare TEXT,
                extras TEXT,
                mta_tax TEXT,
                tip TEXT,
                tolls TEXT,
                improvement TEXT,
                total TEXT,
                congestion TEXT,
                airport_fee TEXT,
                cbd_congestion_fee TEXT
            ) ON COMMIT DROP;
            '''
    else:
        sql = '''
            CREATE TEMP TABLE staging (
                vendor_id TEXT,
                tpep_pickup TEXT,
                tpep_dropoff TEXT,
                passenger_count TEXT,
                trip_distance TEXT,
                ratecode_id TEXT,
                store_and_forward TEXT,
                pu_location_id TEXT,
                do_location_id TEXT,
                payment_type TEXT,
                fare TEXT,
                extras TEXT,
                mta_tax TEXT,
                tip TEXT,
                tolls TEXT,
                improvement TEXT,
                total TEXT,
                congestion TEXT,
                airport_fee TEXT
            ) ON COMMIT DROP;
            '''
    return sql
