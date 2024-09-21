CREATE TABLE IF NOT EXISTS  clean_data.weather.calculo (
    min_temp float,
    max_temp float,
    promedio_temp float);

insert into {{params.name_table}}
select 
min(temp) as min_temp,
max(temp) as max_temp,
avg(temp) as promedio_temp
from RAW_DATA.CONAGUA_WEATHER.CONAGUA_WEATHER_RAW
where HLOC::date = {{params.dt}};
