# Automated Weather ETL Pipeline (v1 & v2)

A production-grade, fault-tolerant ETL pipeline ingesting real-time meteorological data across 5 major Indian metropolitan areas, demonstrating progression from basic local scheduling (v1) to enterprise-grade workflow orchestration with Apache Airflow and PostgreSQL (v2).

---

## Architecture Evolution

### v1 Architecture (Baseline)
```
Open-Meteo REST API -> Python Extraction -> Pandas Transformation -> SQLite Storage -> Schedule Library
```

### v2 Architecture (Enterprise Grade)
```
Open-Meteo REST API
        |
        v
[Apache Airflow DAG]
  |-- Task 1: extract (isolated API extraction with automated retries)
  |-- Task 2: transform (Celsius to Fahrenheit enrichment & typing)
  |-- Task 3: load (idempotent batch UPSERT via psycopg2 into PostgreSQL)
  \-- Task 4: quality_check (asserts zero nulls and non-empty warehouse state)
        |
        v
PostgreSQL Relational Warehouse (WSL2)
```

---

## Tech Stack Comparison

| Feature | v1 (Baseline) | v2 (Production Standard) |
| :--- | :--- | :--- |
| **Orchestration** | Python schedule loop | **Apache Airflow DAG** (Airflow 3 / 2.9+) |
| **Storage Engine** | SQLite (file-based) | **PostgreSQL 18** (Relational RDBMS) |
| **Execution Environment** | Windows native script | **WSL2 (Linux)** isolated virtual environment |
| **Data Ingestion** | Full file append | **Idempotent batch load** (ON CONFLICT DO NOTHING) |
| **Fault Tolerance** | Script-level try/except | **Task-level retries**, SLA tracking & UI monitoring |
| **Data Quality** | Manual inspection | **Automated post-load assertion checks** |

---

## Project Structure

```
data_pipeline/
|-- v1/
|   |-- pipeline.py         # SQLite + Schedule pipeline
|   |-- analyze.py          # SQLite window function analytics
|   \-- requirements.txt
|-- v2/
|   |-- weather_etl_dag.py  # Production Apache Airflow DAG
|   |-- schema.sql          # PostgreSQL DDL with composite primary key
|   |-- analyze_v2.py       # PostgreSQL CTE & window function analytics
|   \-- requirements.txt
\-- README.md
```

---

## Database Schema (v2 PostgreSQL)

```sql
CREATE TABLE IF NOT EXISTS weather_history (
    city VARCHAR(50) NOT NULL,
    time_stamp TIMESTAMP NOT NULL,
    temperature NUMERIC(4, 1) NOT NULL,
    temp_f NUMERIC(4, 1) NOT NULL,
    windspeed NUMERIC(4, 1) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, time_stamp)
);

CREATE INDEX IF NOT EXISTS idx_weather_history_timestamp ON weather_history(time_stamp DESC);
```

---

## Quick Start (v2)

### 1. Database Setup
```bash
sudo service postgresql start
psql -U preet -d weather_db -f v2/schema.sql
```

### 2. Airflow Deployment
```bash
cp v2/weather_etl_dag.py ~/airflow/dags/
airflow standalone
```
Navigate to `http://localhost:8080`, unpause `weather_etl_pipeline_v2`, and trigger execution.

### 3. Run Analytics
```bash
python3 v2/analyze_v2.py
```

---

## Sample Analytical Query (Latest Metros Snapshot)

```sql
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
```
