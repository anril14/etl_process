import duckdb

from airflow.sdk.bases.operator import AirflowException
from airflow.sdk import dag, task
from utils.connections import get_duckdb_connection


def _update_data_mart():
    with get_duckdb_connection(db_schemas=['ods', 'dm']) as con:
        con.begin()
        try:
            # TODO: Incremental load

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
