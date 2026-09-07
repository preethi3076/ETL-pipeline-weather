import pandas as pd 
import sqlite3 as s
conn= s.connect("weather_data.db")
#the below query is to get avg temp and max windspeed of each city except mumbi 
#query="Select city,round(avg(Temperature), 1) as avg_temperature,max(windspeed) as max_windspeed from weather_history where city!='Mumbi' group by city order by avg(Temperature) desc"

#
# query = """
# WITH RankedWeather AS (
#     SELECT 
#         City,
#         Time_Stamp,
#         Temperature,
#         Temp_F,
#         WindSpeed,
#         ROW_NUMBER() OVER (
#             PARTITION BY City 
#             ORDER BY Time_Stamp DESC ,rowid desc
#         ) AS rn
#     FROM weather_history
#     WHERE City != 'Mumbi'
# )
# SELECT 
#     City,
#     Time_Stamp,
#     Temperature,
#     Temp_F,
#     WindSpeed
# FROM RankedWeather
# WHERE rn = 1
# ORDER BY City;
# """
query='''SELECT City, Time_Stamp, Temperature, Temp_F, WindSpeed FROM weather_history
   WHERE Time_Stamp = (SELECT MAX(Time_Stamp) FROM weather_history)
   ORDER BY City;'''

df = pd.read_sql(query,conn)
print (df)