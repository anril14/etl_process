import duckdb

from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task
from utils.get_env import *


def _update_data_mart():
    with duckdb.connect(database=':memory') as con:
        con.begin()
        try:
            con.execute(f'''attach
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                as ods(type postgres, schema ods)''')

            print('Successfully connected to an ods table')

            con.execute(f'''attach
                'host={POSTGRES_DWH_HOST} 
                port={POSTGRES_DWH_PORT}
                dbname={POSTGRES_DWH_DB} 
                user={POSTGRES_DWH_USER} 
                password={POSTGRES_DWH_PASSWORD}'
                as dm(type postgres, schema dm)''')

            print('Successfully connected to a dm table')

            con.execute('''truncate table dm.daily_metrics
            ''')

            print('Truncated daily_metrics')

            con.execute('''insert into dm.daily_metrics
                select date(tpep_pickup) as date,
                avg(total)::numeric(12, 2) as avg_total,
                avg(tip)::numeric(12, 2) as avg_tip,
                avg(passenger_count)::numeric(12, 2) as avg_passenger_count,
                avg(trip_distance)::numeric(12, 2) as avg_trip_distance
                from ods.taxi_data
                group by date
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
