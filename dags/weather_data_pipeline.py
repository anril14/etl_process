import io
import json
import os
import psycopg2
from airflow.sdk.bases.operator import AirflowException
from dotenv import dotenv_values
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from random import randint
from minio import Minio
import requests

# TODO Добавить контексты в DAG'и
# TODO Поменять датасет на более наполненный измерениями по датам

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

        # create bucket if no one
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Bucket {bucket_name} is created, saving file...')
        else:
            print(f'Bucket {bucket_name} is already created, saving file...')

        # file name from date of measure
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
        return result.object_name, data
    except Exception as e:
        print(e)
        raise AirflowException(e)


def _save_to_stg(ti):
    raw_path, data = ti.xcom_pull(task_ids='save_raw_to_minio')
    if not data['dt'] or not data['name']:
        raise ValueError('Not enough data in response')
    dt = data['dt']
    city = data['name']
    with psycopg2.connect(
            host=os.getenv('POSTGRES_DWH_HOST'),
            port=os.getenv('POSTGRES_DWH_PORT'),
            dbname=os.getenv('POSTGRES_DWH_DB'),
            user=os.getenv('POSTGRES_DWH_USER'),
            password=os.getenv('POSTGRES_DWH_PASSWORD'),
    ) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    insert into stg.weather_data (raw_path, dt, city) 
                    values (%s, %s, %s)
                    ''', (raw_path, dt, city))
        except Exception as e:
            print(e)
            raise ConnectionError
        conn.commit()
        print(f'executed')

# TODO Валидация и трансфер данных в ODS

# dag initialization
with DAG('weather_data_pipeline', start_date=datetime(2024, 12, 1),
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
    save_to_stg = PythonOperator(
        task_id='save_to_stg',
        python_callable=_save_to_stg
    )

    get_request >> save_raw_to_minio >> save_to_stg
