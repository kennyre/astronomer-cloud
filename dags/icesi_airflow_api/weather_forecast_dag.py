import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator  # Import del operador de Snowflake

import logging
from dags.icesi_airflow_api.utils.weather_api import run_weather_forecast_pipeline

# Parámetros de configuración
URL = 'https://smn.conagua.gob.mx/tools/GUI/webservices/index.php?method=3'
AWS_CONN_ID = 'aws_conn_s3'
S3_BUCKET = 'api-airflow'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), 'queries')


utc_now = datetime.utcnow()
date_str = utc_now.strftime('%Y-%m-%d')


# Definimos el DAG usando el decorador @dag
@dag(
    schedule_interval='@daily',  # Ejecutar diariamente
    start_date=days_ago(1),      # Iniciar hace un día
    catchup=False,               # Evitar la ejecución de tareas atrasadas
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['weather', 's3'],      # Etiquetas para categorizar el DAG
    template_searchpath=QUERIES_BASE_PATH
)
def weather_forecast_dag():
    
    # Definimos una tarea con el decorador @task
    @task()
    def fetch_and_save_weather_data():
        s3_key_with_date = f'weather_data/weather_data_{date_str}.csv'
        
        # Ejecutar el pipeline con el nuevo nombre de archivo
        run_weather_forecast_pipeline(
            url=URL,
            aws_conn_id=AWS_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=s3_key_with_date
        )
    
    # Tarea para ejecutar una query que crea tabla
    execute_snowflake_create_table = SnowflakeOperator(
        task_id='execute_snowflake_create_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_table.sql'
    )

    # Tarea para ejecutar una query en Snowflake usando el SnowflakeOperator
    execute_snowflake_query = SnowflakeOperator(
        task_id='execute_snowflake_query',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='weather_forecast_query.sql',  # Ruta del archivo SQL dentro del template_searchpath
        params={
            'dt': date_str  # Parametro para la query
        }
    )
    
    # Definir dependencias entre las tareas
    execute_snowflake_create_table >> fetch_and_save_weather_data() >> execute_snowflake_query

# Instancia del DAG
dag = weather_forecast_dag()
