import requests
import json
import gzip
import io
import pandas as pd
import logging
import tempfile
import os
import snowflake.connector

class WeatherForecast:
    def __init__(self, url, snowflake_conn_params=None, database=None, schema=None, table=None):
        self.url = url
        self.snowflake_conn_params = snowflake_conn_params
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
        df.to_csv(temp_file.name, index=False, header=False)
        logging.info(f"Datos guardados en archivo temporal: {temp_file.name}")
        return temp_file.name

    def load_to_snowflake(self, file_path):
        try:
            # Conectamos a Snowflake utilizando los parámetros dados
            conn = snowflake.connector.connect(**self.snowflake_conn_params)
            cursor = conn.cursor()

            # Especificamos la base de datos y el esquema donde queremos cargar los datos
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")

            # Subimos el archivo temporal a una etapa de Snowflake para que pueda ser importado
            stage_name = f"@%{self.table}"
            cursor.execute(f"PUT file://{file_path} {stage_name}")

            # Construimos y ejecutamos la consulta COPY INTO para cargar los datos en la tabla
            copy_into_query = f"""
                COPY INTO {self.table}
                FROM {stage_name}/datos_snowflake_batch.csv
                FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR='CONTINUE'
            """
            cursor.execute(copy_into_query)

            # Después de copiar los datos, eliminamos el archivo de la etapa en Snowflake
            cursor.execute(f"REMOVE {stage_name}")
            logging.info("Datos cargados en Snowflake y archivo eliminado de la etapa.")
            cursor.close()
            conn.close()

            # Finalmente, eliminamos el archivo temporal local
            os.remove(file_path)
            logging.info(f"Archivo temporal eliminado: {file_path}")
            return True
        except Exception as e:
            # Si ocurre algún error durante la carga en Snowflake, lo registramos
            logging.error(f"Error al cargar datos en Snowflake: {str(e)}")
            return False

def run_weather_forecast_pipeline(url, snowflake_conn_params, database, schema, table):
    # Creamos una instancia de WeatherForecast con los parámetros necesarios
    weather_forecast = WeatherForecast(
        url, snowflake_conn_params=snowflake_conn_params, 
        database=database, schema=schema, table=table
    )
    # Obtenemos los datos del clima
    data = weather_forecast.fetch_weather_data()
    if data is not None:
        # Guardamos los datos en un archivo CSV temporal
        temp_file_path = weather_forecast.save_to_temp_csv(data)
        # Cargamos el archivo CSV en Snowflake
        weather_forecast.load_to_snowflake(temp_file_path)
