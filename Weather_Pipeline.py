import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path='/opt/airflow/.env')

# API & DB config
API_KEY = os.getenv('OPENWEATHER_API_KEY')
RAW_CITIES = os.getenv("CITIES", "")
CITIES = RAW_CITIES.split(";") if RAW_CITIES else []

POSTGRES_CONN = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@" \
                f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')

def fetch_weather_data(**kwargs):   
    ### Fetch weather data from OpenWeatherMap API
    results = []
    for city in CITIES:
        try:
            city = city.replace("_", " ")  # Replace spaces with underscores
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=10)
            # response.raise_for_status()
            data = response.json()

            if data.get('cod') != 200:
                print(f"Error fetching data for {city}: {data.get('message', 'Unknown error')}")
                continue
            results.append(data)
        except requests.RequestException as e:
            print(f"Request error for {city}: {e}")
            continue

    if not results:
        raise ValueError(f"Error fetching data in {city}: {e}")

    # Save into Json file
    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    raw_path = f"/tmp/weather_raw_{ts}.json"
    with open(raw_path, 'w') as f:
        json.dump(results, f)

    kwargs['ti'].xcom_push(key='raw_path', value=raw_path)

def upload_to_minio(**kwargs):
    ### Upload JSON file to MinIO
    raw_path = kwargs['ti'].xcom_pull(key='raw_path')
    
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY,
                      config=Config(signature_version='s3v4'))
    
    object_name = f"raw/{os.path.basename(raw_path)}"
    s3.upload_file(raw_path, MINIO_BUCKET, object_name)

def process_and_save_postgres(**kwargs):
    raw_file_path = kwargs['ti'].xcom_pull(task_ids='fetch_weather', key='raw_path')
    with open(raw_file_path, 'r') as f:
        data_list = json.load(f)

    records = []
    for data in data_list:
        records.append({
            'city': data['name'],
            'timestamp': datetime.utcfromtimestamp(data['dt']),
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed']
        })

    df = pd.DataFrame(records)

    engine = create_engine(POSTGRES_CONN)
    df.to_sql('weather_data', engine, if_exists='append', index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "weather_pipeline",
    default_args=default_args,
    description="Thu thập dữ liệu thời tiết ở Việt Nam",
    schedule_interval="0 */3 * * *",
    catchup=False,
) as dags:
    
    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=process_and_save_postgres,
        provide_context=True
    )

    fetch_task >> upload_task >> save_task