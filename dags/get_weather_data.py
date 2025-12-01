import io
import json
import os

from airflow.sdk.bases.operator import AirflowException
from dotenv import dotenv_values
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from random import randint
from minio import Minio
import requests


# get request to openweather api
def _get_request(lat, lon, api_key: str):
    try:
        r = requests.get(url=f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}')
        return r.json()
    except Exception:
        raise Exception('Get request error')


# saving raw json to minio bucket
def _save_raw_to_minio(ti):
    try:
        data = ti.xcom_pull(task_ids='get_request')
        if not data:
            raise ValueError('No data from prev task')

        # cast to json format
        data_bytes = json.dumps(data).encode('utf-8')
        print(os.getenv('MINIO_ENDPOINT'))
        client = Minio(
            endpoint=str(os.getenv('MINIO_ENDPOINT')),
            access_key=str(os.getenv('MINIO_ACCESS_KEY')),
            secret_key=str(os.getenv('MINIO_SECRET_KEY')),
            secure=False
        )
        bucket_name = str(os.getenv('MINIO_BUCKET_NAME'))

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Bucket {bucket_name} is created, saving file...')
        else:
            print(f'Bucket {bucket_name} is already created, saving file...')

        city = str(data['name']).lower()
        unix = str(data['dt'])
        date = datetime.fromtimestamp(data['dt'])
        day = date.day
        month = date.month
        year = date.year
        hour = date.hour
        minute = date.minute

        result = client.put_object(bucket_name=bucket_name,
                                   object_name=f'raw/{city}/{year}/{month:02d}/'
                                               f'{day:02d}/{hour:02d}_{minute:02d}_{unix}.json',
                                   data=io.BytesIO(data_bytes),
                                   content_type='application/json',
                                   length=len(data_bytes),
                                   )
        print(
            f'created {result.object_name} object; etag: {result.etag}, '
            f'version-id: {result.version_id}',
        )
        return f'Successfully saved to minio: {result.object_name}'
    except Exception as e:
        print(e)
        raise AirflowException(e)


def _validate_data(ti):
    # TODO First-stage validate
    data = ti.xcom_pull(task_ids='get_weather')
    return data


# dag initialization
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

    get_request >> save_raw_to_minio
