import warnings
warnings.filterwarnings('ignore', category=UserWarning)
import pandas as pd
import psycopg2

DB_CONFIG = {
    "dbname": "weather_db",
    "user": "preet",
    "password": "lenka@2006",
    "host": "localhost",
    "port": 5432
}

def run_analytics():
    conn = psycopg2.connect(**DB_CONFIG)
    
    print("--- 1. Latest Weather Snapshot per Metro (Window Function CTE) ---")
    query_latest = """
    WITH RankedWeather AS (
        SELECT 
            city,
            time_stamp,
            temperature,
            temp_f,
            windspeed,
            ROW_NUMBER() OVER (
                PARTITION BY city 
                ORDER BY time_stamp DESC
            ) AS rn
        FROM weather_history
    )
    SELECT city, time_stamp, temperature, temp_f, windspeed
    FROM RankedWeather
    WHERE rn = 1
    ORDER BY temperature DESC;
    """
    df_latest = pd.read_sql(query_latest, conn)
    print(df_latest.to_string(index=False))

    print("\n--- 2. City Aggregates (AVG, MAX, MIN) ---")
    query_agg = """
    SELECT 
        city,
        ROUND(AVG(temperature), 2) AS avg_temp_c,
        ROUND(MAX(temperature), 2) AS max_temp_c,
        ROUND(MIN(temperature), 2) AS min_temp_c,
        ROUND(MAX(windspeed), 2) AS max_windspeed,
        COUNT(*) AS total_snapshots
    FROM weather_history
    GROUP BY city
    ORDER BY avg_temp_c DESC;
    """
    df_agg = pd.read_sql(query_agg, conn)
    print(df_agg.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    run_analytics()
