import requests
import json
import gzip
import io
import pandas as pd
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class WeatherForecast:
    def __init__(self, url, aws_conn_id='aws_default', s3_bucket=None):
        self.url = url
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.s3_bucket = s3_bucket

    def fetch_weather_data(self):
        try:
            # Hacer la solicitud a la API
            response = requests.get(self.url)
            logging.info(f"HTTP Status Code: {response.status_code}")
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch data: {response.content}")
                return None
            
            # Descomprimir el archivo GZIP
            gz_stream = io.BytesIO(response.content)
            with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
                json_data = json.load(f)
            
            # Verificar si los datos están vacíos
            if not json_data:
                logging.error("JSON data is empty")
                return None

            # Convertir JSON a DataFrame de pandas
            df_data = [item for item in json_data]
            data = pd.DataFrame(df_data)
            return data

        except Exception as e:
            logging.error(f"Error fetching weather data: {str(e)}")
            return None

    def get_latest_records(self, data):
        if data is None:
            logging.error("Cannot get latest records as the data is None.")
            return None

        # Procesar los datos y obtener los registros más recientes
        data['hloc'] = pd.to_datetime(data['hloc'])
        last_values = data.sort_values(by='hloc').groupby('nmun', as_index=False).last()
        return last_values

    def save_to_s3(self, df, s3_key):
        if df is None:
            logging.error("Cannot save data to S3 as DataFrame is None.")
            return False

        try:
            # Convertir DataFrame a CSV y subir a S3
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            self.s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            
            logging.info(f"Successfully saved data to S3: {self.s3_bucket}/{s3_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to save data to S3: {str(e)}")
            return False

def run_weather_forecast_pipeline(url, aws_conn_id, s3_bucket, s3_key):
    # Instanciar la clase y ejecutar el pipeline completo
    weather_forecast = WeatherForecast(url, aws_conn_id=aws_conn_id, s3_bucket=s3_bucket)
    data = weather_forecast.fetch_weather_data()
    latest_data = weather_forecast.get_latest_records(data)
    weather_forecast.save_to_s3(latest_data, s3_key)
