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
        self.url = url
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table = table

    def fetch_weather_data(self):
        try:
            # Hacemos la solicitud a la API para obtener los datos del clima
            response = requests.get(self.url)
            logging.info(f"Código de estado HTTP: {response.status_code}")
            
            # Verificamos si la solicitud fue exitosa (código 200)
            if response.status_code != 200:
                logging.error(f"Error al obtener datos: {response.content}")
                return None
            
            # Si la solicitud fue exitosa, descomprimimos el archivo GZIP de la respuesta
            gz_stream = io.BytesIO(response.content)
            with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
                json_data = json.load(f)
            
            # Si no se obtienen datos, lo registramos como un error
            if not json_data:
                logging.error("Los datos JSON están vacíos")
                return None

            # Convertimos el JSON en un DataFrame de pandas, que es más fácil de manejar
            df_data = [item for item in json_data]
            data = pd.DataFrame(df_data)
            return data

        except Exception as e:
            # Si ocurre un error durante el proceso, lo registramos
            logging.error(f"Error al obtener datos del clima: {str(e)}")
            return None

    def save_to_temp_csv(self, df):
        # Guardamos el DataFrame en un archivo CSV temporal sin encabezados (header=False)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(temp_file.name, index=False, header=True)  # Guardamos con encabezado para crear la tabla
        logging.info(f"Datos guardados en archivo temporal: {temp_file.name}")
        return temp_file.name

    def create_table_in_snowflake(self, df):
        # Creamos un hook de Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Construimos la instrucción SQL para crear la tabla según el encabezado y tipos de datos del DataFrame
        columns = []
        for column in df.columns:
            if pd.api.types.is_integer_dtype(df[column]):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[column]):
                col_type = "FLOAT"
            else:
                col_type = "TEXT"
            columns.append(f"{column} {col_type}")

        # Creamos la tabla en Snowflake con el SQL generado
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({', '.join(columns)});"
        cursor.execute(f"USE DATABASE {self.database}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Tabla {self.table} creada en Snowflake.")

    def load_to_snowflake(self, file_path):
        try:
            # Creamos un hook de Snowflake usando la conexión configurada en Airflow
            snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
            conn = snowflake_hook.get_conn()
            cursor = conn.cursor()

            # Establecemos la base de datos y el esquema donde queremos cargar los datos
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")

            # Subimos el archivo temporal a una etapa de Snowflake usando el nombre dinámico del archivo
            stage_name = f"@%{self.table}"
            cursor.execute(f"PUT file://{file_path} {stage_name}")

            copy_into_query = f"""
                COPY INTO {self.table}
                FROM {stage_name}/{os.path.basename(file_path)}
                FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR='CONTINUE'
            """
            cursor.execute(copy_into_query)

            # Eliminamos el archivo de la etapa de Snowflake
            cursor.execute(f"REMOVE {stage_name}")
            logging.info("Datos cargados en Snowflake y archivo eliminado de la etapa.")
            cursor.close()
            conn.close()

            # Eliminamos el archivo temporal local
            os.remove(file_path)
            logging.info(f"Archivo temporal eliminado: {file_path}")
            return True
        except Exception as e:
            # Si ocurre algún error durante la carga en Snowflake, lo registramos
            logging.error(f"Error al cargar datos en Snowflake: {str(e)}")
            return False


def run_weather_forecast_pipeline(url, snowflake_conn_id, database, schema, table):
    # Creamos una instancia de WeatherForecast con la conexión de Airflow y los parámetros necesarios
    weather_forecast = WeatherForecast(
        url, snowflake_conn_id=snowflake_conn_id, 
        database=database, schema=schema, table=table
    )
    # Obtenemos los datos del clima
    data = weather_forecast.fetch_weather_data()
    if data is not None:
        # Creamos la tabla automáticamente según el esquema de los datos
        weather_forecast.create_table_in_snowflake(data)
        # Guardamos los datos en un archivo CSV temporal
        temp_file_path = weather_forecast.save_to_temp_csv(data)
        # Cargamos el archivo CSV en Snowflake
        weather_forecast.load_to_snowflake(temp_file_path)
