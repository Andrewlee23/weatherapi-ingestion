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

def get_or_create_location(conn, query: str, name: str | None = None) -> int:
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wx_location(query, name)
            VALUES (%s, %s)
            ON CONFLICT (query) DO UPDATE
              SET name = COALESCE(EXCLUDED.name, wx_location.name)
            RETURNING id;
        """, (query, name))
        return cur.fetchone()[0]

def upsert_current(conn, loc_id: int, payload: dict):
    current = payload["current"]
    epoch = current.get("last_updated_epoch")
    obs_time = datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch else dtparse.parse(current["last_updated"])
    row = (
        loc_id,
        obs_time,
        current.get("temp_f"),
        current.get("condition", {}).get("text"),
        current.get("condition", {}).get("code"),
        current.get("wind_mph"),
        current.get("gust_mph"),
        current.get("precip_in"),
        current.get("cloud"),
        json.dumps(current),
    )
    with conn, conn.cursor() as cur:
        cur.execute("""
        INSERT INTO wx_current(loc_id, obs_time_utc, temp_f, cond_text, cond_code, wind_mph, gust_mph, precip_in, cloud, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (loc_id, obs_time_utc) DO UPDATE SET
          temp_f = EXCLUDED.temp_f,
          cond_text = EXCLUDED.cond_text,
          cond_code = EXCLUDED.cond_code,
          wind_mph = EXCLUDED.wind_mph,
          gust_mph = EXCLUDED.gust_mph,
          precip_in = EXCLUDED.precip_in,
          cloud = EXCLUDED.cloud,
          raw = EXCLUDED.raw;
        """, row)

def upsert_forecast_days(conn, loc_id: int, payload: dict):
    days = payload["forecast"]["forecastday"]
    rows = []
    for d in days:
        day = d["day"]
        rows.append((
            loc_id,
            dtparse.parse(d["date"]).date(),
            day.get("maxtemp_f"),
            day.get("mintemp_f"),
            day.get("maxwind_mph"),
            day.get("totalprecip_in"),
            day.get("totalsnow_cm"),
            day.get("condition", {}).get("text"),
            day.get("condition", {}).get("code"),
            int(day.get("daily_chance_of_rain") or 0),
            int(day.get("daily_chance_of_snow") or 0),
            json.dumps(day)
        ))
    if not rows:
        return
    with conn, conn.cursor() as cur:
        execute_values(cur, """
        INSERT INTO wx_forecast_day(
          loc_id, forecast_date, maxtemp_f, mintemp_f, maxwind_mph, totalprecip_in, totalsnow_cm,
          cond_text, cond_code, daily_chance_of_rain, daily_chance_of_snow, raw
        ) VALUES %s
        ON CONFLICT (loc_id, forecast_date) DO UPDATE SET
          maxtemp_f = EXCLUDED.maxtemp_f,
          mintemp_f = EXCLUDED.mintemp_f,
          maxwind_mph = EXCLUDED.maxwind_mph,
          totalprecip_in = EXCLUDED.totalprecip_in,
          totalsnow_cm = EXCLUDED.totalsnow_cm,
          cond_text = EXCLUDED.cond_text,
          cond_code = EXCLUDED.cond_code,
          daily_chance_of_rain = EXCLUDED.daily_chance_of_rain,
          daily_chance_of_snow = EXCLUDED.daily_chance_of_snow,
          raw = EXCLUDED.raw;
        """, rows)

def insert_alerts(conn, loc_id: int, payload: dict) -> int:
    alerts = payload.get("alerts", {}).get("alert", []) or []
    if not alerts:
        return 0
    rows = []
    for a in alerts:
        rows.append((
            loc_id,
            a.get("headline"),
            a.get("severity"),
            a.get("areas"),
            a.get("certainty"),
            a.get("event"),
            a.get("note"),
            dtparse.parse(a["effective"]) if a.get("effective") else None,
            dtparse.parse(a["expires"]) if a.get("expires") else None,
            json.dumps(a)
        ))
    with conn, conn.cursor() as cur:
        execute_values(cur, """
        INSERT INTO wx_alert(
          loc_id, headline, severity, areas, certainty, event, note, effective, expires, raw
        ) VALUES %s;
        """, rows)
    return len(rows)

def run():
    conn = psycopg2.connect(PG_DSN)
    ensure_schema(conn)
    try:
        for q in LOCATIONS:
            payload = get_forecast(q, days=FORECAST_DAYS)
            loc_id = get_or_create_location(conn, q, name=None)
            upsert_current(conn, loc_id, payload)
            upsert_forecast_days(conn, loc_id, payload)
            insert_alerts(conn, loc_id, payload)
            if RATE_SLEEP > 0:
                time.sleep(RATE_SLEEP)
    finally:
        conn.close()

if __name__ == "__main__":
    run()
