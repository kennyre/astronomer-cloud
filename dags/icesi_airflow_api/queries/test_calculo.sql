create if not exists table clean_data.weather.calculo (
    min_temp float,
    max_temp float,
    promedio_temp float);

insert into clean_data.weather.calculo  
select 
min(temp) as min_temp,
max(temp) as max_temp,
avg(temp) as promedio_temp
from RAW_DATA.CONAGUA_WEATHER.CONAGUA_WEATHER_RAW
where HLOC::date = {{params.dt}};
