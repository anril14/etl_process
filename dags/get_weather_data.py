import os
from dotenv import dotenv_values
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from random import randint
import requests


def _get_request(lat, lon, api_key: str):
    try:
        r = requests.get(url=f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}')
        return r.json()
    except Exception:
        raise Exception('Error')


def _save_raw_to_minio(ti):
    # TODO Save to minio
    data = ti.xcom_pull(task_ids='get_weather')
    return data


def _validate_data(ti):
    # TODO First-stage validate
    data = ti.xcom_pull(task_ids='get_weather')
    return data


with DAG('get_weather_data', start_date=datetime(2024, 12, 1),
         schedule='@daily', catchup=False) as dag:
    get_request = PythonOperator(
        task_id='get_request',
        python_callable=_get_request,
        op_kwargs={'lat': '51.66',
                   'lon': '39.19',
                   'api_key': str(os.getenv('OPENWEATHER_API_KEY'))
                   }
    )
    save_raw_to_minio = PythonOperator(
        task_id='save_raw_to_minio',
        python_callable=_save_raw_to_minio
    )

    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=_validate_data,
    )
