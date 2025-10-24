import os
import json
import time
import requests
from datetime import datetime, timezone
from dateutil import parser as dtparse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.environ["WEATHERAPI_KEY"]# WeatherAPI key
PG_DSN = os.environ["PG_DSN"]# e.g. postgresql://user:pass@host:5432/db (pgadmin)
LOCATIONS = [s.strip() for s in os.getenv("WX_LOCATIONS", "Rochester,NY").split(",") if s.strip()]
FORECAST_DAYS = int(os.getenv("WX_DAYS", "3"))
RATE_SLEEP = float(os.getenv("WX_RATE_SLEEP", "0.6"))  

BASE = "https://api.weatherapi.com/v1"

def get_forecast(location: str, days: int = 3) -> dict:
    url = f"{BASE}/forecast.json"
    params = {"key": API_KEY, "q": location, "days": days, "alerts": "yes", "aqi": "no"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS wx_location (
          id SERIAL PRIMARY KEY,
          query TEXT UNIQUE,
          name TEXT
        );
        CREATE TABLE IF NOT EXISTS wx_current (
          id BIGSERIAL PRIMARY KEY,
          loc_id INT REFERENCES wx_location(id),
          obs_time_utc TIMESTAMPTZ NOT NULL,
          temp_f NUMERIC,
          cond_text TEXT,
          cond_code INT,
          wind_mph NUMERIC,
          gust_mph NUMERIC,
          precip_in NUMERIC,
          cloud INT,
          raw JSONB,
          UNIQUE (loc_id, obs_time_utc)
        );
        CREATE TABLE IF NOT EXISTS wx_forecast_day (
          id BIGSERIAL PRIMARY KEY,
          loc_id INT REFERENCES wx_location(id),
          forecast_date DATE NOT NULL,
          maxtemp_f NUMERIC,
          mintemp_f NUMERIC,
          maxwind_mph NUMERIC,
          totalprecip_in NUMERIC,
          totalsnow_cm NUMERIC,
          cond_text TEXT,
          cond_code INT,
          daily_chance_of_rain INT,
          daily_chance_of_snow INT,
          raw JSONB,
          UNIQUE (loc_id, forecast_date)
        );
        CREATE TABLE IF NOT EXISTS wx_alert (
          id BIGSERIAL PRIMARY KEY,
          loc_id INT REFERENCES wx_location(id),
          headline TEXT,
          severity TEXT,
          areas TEXT,
          certainty TEXT,
          event TEXT,
          note TEXT,
          effective TIMESTAMPTZ,
          expires TIMESTAMPTZ,
          raw JSONB,
          ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
