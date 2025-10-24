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
