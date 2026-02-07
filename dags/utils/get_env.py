"""
Getting environment variables from .env in project root
"""
import os

# minio
MINIO_ACCESS_KEY = str(os.getenv('MINIO_ACCESS_KEY'))
MINIO_SECRET_KEY = str(os.getenv('MINIO_SECRET_KEY'))
MINIO_ENDPOINT = str(os.getenv('MINIO_ENDPOINT'))
MINIO_BUCKET_NAME = str(os.getenv('MINIO_BUCKET_NAME'))

# postgres
POSTGRES_DWH_HOST = os.getenv('POSTGRES_DWH_HOST')
POSTGRES_DWH_PORT = os.getenv('POSTGRES_DWH_PORT')
POSTGRES_DWH_DB = os.getenv('POSTGRES_DWH_DB')
POSTGRES_DWH_USER = os.getenv('POSTGRES_DWH_USER')
POSTGRES_DWH_PASSWORD = os.getenv('POSTGRES_DWH_PASSWORD')
