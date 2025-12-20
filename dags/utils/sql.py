def get_duckdb_table_sql(year: str):
    if int(year) >= 2025:
        sql = f'''
            CREATE TABLE staging (
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
            CREATE TABLE staging (
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