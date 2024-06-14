from time import sleep
from airflow import DAG, task  # Importa a classe DAG e task
from datetime import datetime

@dag(schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False)
def etl_exemplo1():
    
    
