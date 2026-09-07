import logging 
import sqlite3
import pandas as pd
import requests
import schedule 
import time

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    datefmt = '%y-%m-%d %H:%M:%S'
)

CITIES = [
    {"name": "Delhi", "lat": 28.6139, "lon": 77.2090},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"name": "Chennai", "lat": 13.0827, "lon": 80.2707},
    {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639}
]
DB_PATH = "weather_data.db"

def extract(cities:list):
    logging.info("Starting extraction for %d target cities..", len(cities))
    raw_records = []
    for city in cities:
        try:
            url = (
                "https://api.open-meteo.com/v1/forecast?"
                f"latitude={city['lat']}&longitude={city['lon']}&current_weather=true&current_weather=true&timezone=Asia%2FKolkata"
            )
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            weather = response.json()["current_weather"]
            raw_records.append({
                "city": city["name"],
                "timestamp": weather["time"],
                "temp_c": weather["temperature"],
                "windspeed": weather["windspeed"],
            })
        except (requests.RequestException, KeyError) as error:
            logging.error("Could not fetch %s: %s", city["name"], error)
    return raw_records

def transform(raw_records):
    if not raw_records:
        return pd.DataFrame(columns=["City", "Time_Stamp", "Temperature", "Temp_F", "WindSpeed"])
    processed = []
    for r in raw_records:
        temp_f = round((r["temp_c"] * 9/5) + 32, 1)
        processed.append({
            "City": r["city"],
            "Time_Stamp": r["timestamp"],
            "Temperature": r["temp_c"],
            "Temp_F": temp_f,
            "WindSpeed": r["windspeed"]
        })
    return pd.DataFrame(processed)
def load(df: pd.DataFrame, db_path: str) -> None:
    """Persists transformed data into the SQLite database."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping load phase.")
        return
    logging.info("Loading %d records into database: %s", len(df), db_path)
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(weather_history)")}
        if columns and "Temp_F" not in columns:
            conn.execute("ALTER TABLE weather_history ADD COLUMN Temp_F REAL")

        df = df.drop_duplicates(subset=["City", "Time_Stamp"])
        if columns:
            existing = pd.read_sql(
                "SELECT City, Time_Stamp FROM weather_history", conn
            )
            df = df.merge(
                existing.assign(_exists=True),
                on=["City", "Time_Stamp"],
                how="left",
            )
            df = df[df["_exists"].isna()].drop(columns="_exists")

        if df.empty:
            logging.info("No new records to load.")
            return
        df.to_sql("weather_history", conn, if_exists="append", index=False)
    logging.info("Database load completed successfully: %d new records.", len(df))
def run_pipeline():
    """Main Orchestrator."""
    logging.info("==========================================")
    logging.info("🚀 STARTING WEATHER ETL PIPELINE")
    logging.info("==========================================")
    
    raw_data = extract(CITIES)
    df = transform(raw_data)
    load(df, DB_PATH)
    
    logging.info("==========================================")
    logging.info("✅ PIPELINE EXECUTION COMPLETED")
    logging.info("==========================================")
if __name__ == "__main__":
    run_pipeline() 
    schedule.every(15).minutes.do(run_pipeline)
    
    logging.info("Scheduler active. Pipeline will run every 15 minutes.")
    logging.info("Press CTRL+C to stop.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

res='SELECT City, Time_Stamp FROM weather_history ORDER BY rowid DESC LIMIT 5;'
df = pd.read_sql(res,sqlite3.connect("weather_data.db"))
print(df)