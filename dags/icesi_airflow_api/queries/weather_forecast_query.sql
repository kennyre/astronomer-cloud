COPY INTO RAW_DATA.CONAGUA_WEATHER.weather_forecast_raw
FROM '@RAW_DATA.CONAGUA_WEATHER.stage_raw_data/weather_data/weather_data_{{params.dt}}.csv'
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
        