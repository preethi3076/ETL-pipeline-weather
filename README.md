# Automated Weather ETL Pipeline

A production-grade, automated ETL pipeline that ingests real-time weather data for 5 major Indian metros every 15 minutes, stores it in a structured time-series database, and supports SQL analytics.

## Architecture

REST API (Open-Meteo) → Extract → Transform → Load → SQLite Database
                                                           |
                                                Scheduled every 15 minutes

## Tech Stack

- Python — Core pipeline logic
- Pandas — Data transformation and enrichment
- SQLite — Persistent time-series storage
- Schedule — Automated pipeline orchestration
- Open-Meteo REST API — Real-time weather data (no API key required)

## Features

- Fault-tolerant extraction with per-city error handling
- Schema evolution via ALTER TABLE for new columns
- Data enrichment (Celsius to Fahrenheit conversion)
- Structured logging with timestamps and log levels
- Duplicate-safe loading using append strategy
- SQL analytics with Window Functions and CTEs

## Version Roadmap

| Version | Status | Stack |
|---------|--------|-------|
| v1 — SQLite + Scheduler | Complete | Python, Pandas, SQLite |
| v2 — PostgreSQL + Airflow | In Progress | PostgreSQL, Apache Airflow, WSL2 |

## Quick Start

pip install -r requirements.txt
python v1/pipeline.py

## Sample Analytics Query

WITH RankedWeather AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY City
            ORDER BY Time_Stamp DESC
        ) AS rn
    FROM weather_history
)
SELECT City, Time_Stamp, Temperature, Temp_F, WindSpeed
FROM RankedWeather
WHERE rn = 1;
