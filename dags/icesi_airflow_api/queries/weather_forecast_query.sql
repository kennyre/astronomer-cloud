COPY INTO RAW_DATA.CONAGUA_WEATHER.weather_forecast_raw
FROM 's3://{S3_BUCKET}/weather_data/{{params.dt}}.csv'
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
        