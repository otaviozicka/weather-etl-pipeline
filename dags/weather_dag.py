from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pathlib import Path
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

from extract_date import extract_weather_data
from load_data import load_weather_data
from transform_data import data_transformations
from dotenv import load_dotenv
from gold_transforms import run_gold_pipeline

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

api_key = os.getenv('api_key')
url = f'https://api.openweathermap.org/data/2.5/weather?q=Cascavel,BR&units=metric&lang=pt_br&appid={api_key}'

@dag(
    dag_id='first_weather_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL - CLima Cascavel',
    schedule='0 */1 * * * ',
    start_date=datetime(2026, 3, 19),
    catchup=False, ##relacioando a execução.
    tags=['weather', 'cascavel', 'feemdeus']
)
def weather_pipeline():
    
    @task
    def extract():
        api_key = os.getenv('api_key')
        url = f'https://api.openweathermap.org/data/2.5/weather?q=Cascavel,BR&units=metric&lang=pt_br&appid={api_key}'
        extract_weather_data(url)
        
    @task
    def transform():
        df = data_transformations()
        df.to_parquet('/opt/airflow/data/temp_data.parquet', index=False)
        
    @task 
    def load():
        import pandas as pd
        df = pd.read_parquet('/opt/airflow/data/temp_data.parquet')
        load_weather_data('cascacity_weather', df)
        
    @task
    def gold():
        from load_data import get_engine
        engine = get_engine()
        run_gold_pipeline(engine)

    extract() >> transform() >> load() >> gold()    

weather_pipeline()
