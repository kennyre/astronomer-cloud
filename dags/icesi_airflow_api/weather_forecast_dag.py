import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dags.icesi_airflow_api.utils.weather_api import run_weather_forecast_pipeline

# Definimos las configuraciones básicas para la ejecución
URL = 'https://smn.conagua.gob.mx/tools/GUI/webservices/index.php?method=3'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'  # ID de conexión configurado en Airflow
DATABASE = 'DEV_ICESI'  # Nombre de la base de datos en Snowflake
SCHEMA = 'SYSTEM_RECOMMENDATION'          # Nombre del esquema en Snowflake
TABLE = 'CONAGUA_WEATHER_RAW'              # Nombre de la tabla de destino en Snowflake
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), 'queries')


# Definimos el DAG usando el decorador @dag de Airflow
@dag(
    schedule_interval='@daily',  # Configuramos el DAG para que se ejecute todos los días
    start_date=days_ago(1),      # Se iniciará desde el día anterior para pruebas
    catchup=False,               # Evitamos la ejecución de tareas atrasadas
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['weather', 'snowflake'],  # Etiquetas para categorizar el DAG en Airflow
    template_searchpath=QUERIES_BASE_PATH  # Carpeta donde Airflow busca archivos SQL
)
def weather_forecast_dag():
    
    # Tarea principal que obtiene los datos de clima y los guarda en Snowflake
    @task()
    def fetch_and_save_weather_data():
        # Ejecutamos el pipeline para obtener los datos del clima y guardarlos en Snowflake
        run_weather_forecast_pipeline(
            url=URL,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE
        )
    
    execute_snowflake_create_table = SnowflakeOperator(
        task_id='execute_snowflake_create_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='limpiar.sql'
    )
    
    # Establecemos el flujo de tareas
    # Primero creamos la tabla, luego cargamos los datos y finalmente ejecutamos la consulta adicional
    fetch_and_save_weather_data() >> execute_snowflake_create_table

# Instanciamos el DAG
dag = weather_forecast_dag()
