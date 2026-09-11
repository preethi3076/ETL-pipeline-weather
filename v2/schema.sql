-- V2 PostgreSQL Target Table Schema
CREATE TABLE IF NOT EXISTS weather_history (
    city VARCHAR(50) NOT NULL,
    time_stamp TIMESTAMP NOT NULL,
    temperature NUMERIC(4, 1) NOT NULL,
    temp_f NUMERIC(4, 1) NOT NULL,
    windspeed NUMERIC(4, 1) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, time_stamp)
);

-- Index on time_stamp for optimized analytical time-series slicing
CREATE INDEX IF NOT EXISTS idx_weather_history_timestamp ON weather_history(time_stamp DESC);
