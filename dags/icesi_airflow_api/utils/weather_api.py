import requests
import json
import gzip
import io
import pandas as pd
import logging
import tempfile
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class WeatherForecast:
    def __init__(self, url, snowflake_conn_id, database=None, schema=None, table=None):
        # Configuración básica de la clase
        self.url = url
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table = table

    def fetch_weather_data(self):
        try:
            # Solicitamos datos de clima desde la API
            response = requests.get(self.url)
            logging.info(f"Código de estado HTTP: {response.status_code}")
            
            # Verificamos si la solicitud fue exitosa
            if response.status_code != 200:
                logging.error(f"Error al obtener datos: {response.content}")
                return None
            
            # Descomprimimos y leemos el archivo GZIP recibido desde la API
            gz_stream = io.BytesIO(response.content)
            with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
                json_data = json.load(f)
            
            # Si el archivo está vacío, lo registramos como un error
            if not json_data:
                logging.error("Los datos JSON están vacíos")
                return None

            # Convertimos los datos JSON en un DataFrame de pandas para su manipulación
            df_data = [item for item in json_data]
            data = pd.DataFrame(df_data)
            return data

        except Exception as e:
            # Capturamos cualquier error que ocurra en el proceso de obtención de datos
            logging.error(f"Error al obtener datos del clima: {str(e)}")
            return None

    def save_to_temp_csv(self, df):
        # Guardamos el DataFrame en un archivo CSV temporal sin encabezado (header=False)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(temp_file.name, index=False, header=False)  # El encabezado no se guarda en el archivo
        logging.info(f"Datos guardados en archivo temporal sin encabezado: {temp_file.name}")
        return temp_file.name

    def create_table_in_snowflake(self, df):
        # Usamos el hook de Snowflake para manejar la conexión a la base de datos
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Analizamos los tipos de datos del DataFrame y generamos la instrucción CREATE TABLE
        columns = []
        for column in df.columns:
            # Detectamos el tipo de datos de cada columna
            if pd.api.types.is_integer_dtype(df[column]):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[column]):
                col_type = "FLOAT"
            else:
                col_type = "TEXT"  # Guardamos como texto si no es numérico
            columns.append(f"{column} {col_type}")

        # Creamos la tabla en Snowflake según los nombres y tipos de columnas detectados
        create_table_sql = f"CREATE OR REPLACE TABLE {self.table} ({', '.join(columns)});"
        cursor.execute(f"USE DATABASE {self.database}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Tabla {self.table} creada en Snowflake.")

    def load_to_snowflake(self, file_path):
        try:
            # Creamos el hook de Snowflake y obtenemos una conexión
            snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
            conn = snowflake_hook.get_conn()
            cursor = conn.cursor()

            # Especificamos la base de datos y el esquema de destino en Snowflake
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")

            # Cargamos el archivo CSV en una etapa temporal en Snowflake
            stage_name = f"@%{self.table}"
            cursor.execute(f"PUT file://{file_path} {stage_name}")

            # Generamos la consulta COPY INTO para cargar los datos en la tabla
            copy_into_query = f"""
                COPY INTO {self.table}
                FROM {stage_name}/{os.path.basename(file_path)}
                FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR='CONTINUE'
            """
            cursor.execute(copy_into_query)

            # Borramos el archivo de la etapa de Snowflake una vez completada la carga
            cursor.execute(f"REMOVE {stage_name}")
            logging.info("Datos cargados en Snowflake y archivo eliminado de la etapa.")
            cursor.close()
            conn.close()

            # Borramos el archivo temporal local después de que se haya cargado
            os.remove(file_path)
            logging.info(f"Archivo temporal eliminado: {file_path}")
            return True
        except Exception as e:
            # Registramos cualquier error que ocurra durante la carga en Snowflake
            logging.error(f"Error al cargar datos en Snowflake: {str(e)}")
            return False


def run_weather_forecast_pipeline(url, snowflake_conn_id, database, schema, table):
    # Creamos una instancia de WeatherForecast con la conexión de Airflow y parámetros necesarios
    weather_forecast = WeatherForecast(
        url, snowflake_conn_id=snowflake_conn_id, 
        database=database, schema=schema, table=table
    )
    
    # Obtenemos los datos de clima en formato DataFrame
    data = weather_forecast.fetch_weather_data()
    if data is not None:
        # Creamos la tabla en Snowflake usando los nombres y tipos de columnas del DataFrame
        weather_forecast.create_table_in_snowflake(data)
        
        # Guardamos los datos en un archivo CSV temporal sin encabezado
        temp_file_path = weather_forecast.save_to_temp_csv(data)
        
        # Cargamos el archivo CSV en la tabla de Snowflake
        weather_forecast.load_to_snowflake(temp_file_path)
