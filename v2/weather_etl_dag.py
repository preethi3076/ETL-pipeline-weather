from datetime import datetime, timedelta
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

# Target cities across India
CITIES = [
    {"name": "Delhi", "lat": 28.6139, "lon": 77.2090},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"name": "Chennai", "lat": 13.0827, "lon": 80.2707},
    {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639}
]

# PostgreSQL Connection Config
DB_CONFIG = {
    "dbname": "weather_db",
    "user": "preet",
    "password": "lenka@2006",
    "host": "localhost",
    "port": 5432
}

default_args = {
    "owner": "preet",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="weather_etl_pipeline_v2",
    default_args=default_args,
    description="V2 Weather ETL Pipeline ingesting 5 metros into PostgreSQL",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["fintech", "weather", "v2"],
)
def weather_etl():

    @task()
    def extract() -> list:
        """Extract current weather records from Open-Meteo REST API."""
        records = []
        for city in CITIES:
            try:
                url = (
                    f"https://api.open-meteo.com/v1/forecast?"
                    f"latitude={city['lat']}&longitude={city['lon']}&current_weather=true&timezone=Asia%2FKolkata"
                )
                res = requests.get(url, timeout=10)
                res.raise_for_status()
                data = res.json()["current_weather"]
                records.append({
                    "city": city["name"],
                    "timestamp": data["time"],
                    "temp_c": data["temperature"],
                    "windspeed": data["windspeed"]
                })
            except Exception as e:
                logging.error(f"Failed to fetch {city['name']}: {e}")
        if not records:
            raise ValueError("Extraction failed for all target cities!")
        logging.info(f"Successfully extracted {len(records)} city records.")
        return records

    @task()
    def transform(raw_data: list) -> list:
        """Clean, format, and enrich weather data."""
        cleaned_data = []
        for r in raw_data:
            temp_f = round((r["temp_c"] * 9/5) + 32, 1)
            cleaned_data.append({
                "city": r["city"],
                "time_stamp": r["timestamp"],
                "temperature": r["temp_c"],
                "temp_f": temp_f,
                "windspeed": r["windspeed"]
            })
        logging.info(f"Transformed {len(cleaned_data)} records (Celsius -> Fahrenheit enriched).")
        return cleaned_data

    @task()
    def load(transformed_data: list) -> int:
        """Idempotently insert records into PostgreSQL."""
        if not transformed_data:
            logging.warning("No data to load.")
            return 0

        insert_query = """
        INSERT INTO weather_history (city, time_stamp, temperature, temp_f, windspeed)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (city, time_stamp) DO NOTHING;
        """
        records_to_insert = [
            (r["city"], r["time_stamp"], r["temperature"], r["temp_f"], r["windspeed"])
            for r in transformed_data
        ]

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                execute_batch(cur, insert_query, records_to_insert)
            conn.commit()

        logging.info(f"Successfully executed idempotent batch load: {len(records_to_insert)} records.")
        return len(records_to_insert)

    @task()
    def quality_check(loaded_count: int):
        """Verify data integrity and print summary counts."""
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM weather_history WHERE temperature IS NULL OR windspeed IS NULL;")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    raise ValueError(f"Quality Check Failed: Found {null_count} rows with NULL values!")

                cur.execute("SELECT COUNT(*) FROM weather_history;")
                total_rows = cur.fetchone()[0]
                logging.info(f"Data Quality Check PASSED. Total rows in weather_history: {total_rows}")

    raw = extract()
    transformed = transform(raw)
    loaded = load(transformed)
    quality_check(loaded)

weather_etl()
