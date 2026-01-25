import duckdb

from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task
from utils.get_env import *


def _update_data_mart():
    with duckdb.connect(database=':memory') as con:
        con.begin()
        try:
            con.execute(f'''ATTACH
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                AS ods(TYPE postgres, SCHEMA ods)''')

            print('Successfully connected to an ods table')

            con.execute(f'''ATTACH
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                AS dm(TYPE postgres, SCHEMA dm)''')

            #TODO: Incremental load
            print('Successfully connected to a dm table')

            con.execute('''TRUNCATE TABLE dm.daily_metrics
            ''')

            print('Truncated daily_metrics')

            con.execute('''INSERT INTO dm.daily_metrics
                SELECT DATE(tpep_pickup) AS date,
                AVG(total)::NUMERIC(12, 2) AS avg_total,
                AVG(tip)::NUMERIC(12, 2) AS avg_tip,
                AVG(passenger_count)::NUMERIC(12, 2) AS avg_passenger_count,
                AVG(trip_distance)::NUMERIC(12, 2) AS avg_trip_distance
                FROM ods.taxi_data
                GROUP BY date
            ''')
            print('Inserted daily_metrics')
            con.commit()
            print('Executed')

        except Exception as err:
            con.rollback()
            print(err)
            raise AirflowException(err)


# dag initialization
@dag(
    dag_id='recalculate_data_mart',
    schedule=None,
    catchup=False
)
def recalculate_data_mart():
    @task
    def update_data_mart():
        _update_data_mart()

    update_data_mart()


recalculate_data_mart()
