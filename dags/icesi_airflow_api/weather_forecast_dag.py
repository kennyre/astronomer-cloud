from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging

from dags.icesi_airflow_api.utils.weather_api import run_weather_forecast_pipeline

# Parámetros de configuración
URL = 'https://smn.conagua.gob.mx/tools/GUI/webservices/index.php?method=3'
AWS_CONN_ID = 'aws_conn_s3'
S3_BUCKET = 'api-airflowt'

# Definimos el DAG usando el decorador @dag
@dag(
    schedule_interval='@daily',  # Ejecutar diariamente
    start_date=days_ago(1),      # Iniciar hace un día
    catchup=False,               # Evitar la ejecución de tareas atrasadas
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['weather', 's3'],      # Etiquetas para categorizar el DAG
)
def weather_forecast_dag():
    
    # Definimos una tarea con el decorador @task
    @task()
    def fetch_and_save_weather_data(execution_date=None):
        # Utilizamos la fecha de ejecución ({{ ds }}) para crear un archivo único cada día
        date_str = execution_date.strftime('%Y-%m-%d')
        s3_key_with_date = f'weather_data/weather_data_{date_str}.csv'
        
        # Ejecutar el pipeline con el nuevo nombre de archivo
        run_weather_forecast_pipeline(
            url=URL,
            aws_conn_id=AWS_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=s3_key_with_date
        )
    
    # Llamamos a la tarea para crear su dependencia
    fetch_and_save_weather_data()

# Instancia del DAG
dag = weather_forecast_dag()
