from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from random import randint

def _getting_weather():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2024, 12, 1),
         schedule="@daily", catchup=False) as dag:
    get_weather = PythonOperator(
        task_id = "getting_weather",
        python_callable = _getting_weather,
    )