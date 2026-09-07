import requests
import pandas as pd
import sqlite3
 
cities = [
    {"name": "Delhi", "lat": 28.6139, "lon": 77.2090},
    {"name":"Broken city","lat":999.999,"lon":999.999},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"name": "Chennai", "lat": 13.0827, "lon": 80.2707}
]
 
all_weather_data = []
 
for city in cities:
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true&current_weather=true&timezone=Asia%2FKolkata"
        response = requests.get(url)
        data = response.json()
 
        current_weather = data['current_weather']
        temperature = current_weather['temperature']
        temp_f = round((temperature * 9/5) + 32, 1)
        windspeed = current_weather['windspeed']
        time = current_weather['time']
 
        # print(f"{city['name']} - Time: {time}, Windspeed: {windspeed} m/s")
 
        # Transform part: build one row per city
        weather_row = {
            "City": city['name'],
            "Time_Stamp": time,
            "Temperature": temperature,
            "Temp_F": temp_f,
            "WindSpeed": windspeed
        }
        all_weather_data.append(weather_row)
    except Exception as e:
        print(f"Error fetching data for {city['name']}:{e}")
 
# Build the final DataFrame once, after the loop has collected every row
df = pd.DataFrame(all_weather_data)
print(df)
#print(df)

#Load part 
conn = sqlite3.connect("weather_data.db")
existing_columns = {
    row[1] for row in conn.execute("PRAGMA table_info(weather_history)")
}
if existing_columns and "Temp_F" not in existing_columns:
    conn.execute("ALTER TABLE weather_history ADD COLUMN Temp_F REAL")
df.to_sql("weather_history",conn,if_exists="append",index=False)
proff =pd.read_sql("Select * from weather_history",conn)
print(proff)