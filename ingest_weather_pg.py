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
PG_DSN = os.environ["PG_DSN"]# e.g. postgresql://user:pass@host:5432/db (pgadmin deets)
LOCATIONS = [s.strip() for s in os.getenv("WX_LOCATIONS", "Rochester,NY").split(",") if s.strip()]
FORECAST_DAYS = int(os.getenv("WX_DAYS", "3"))
RATE_SLEEP = float(os.getenv("WX_RATE_SLEEP", "0.6"))  

BASE = "https://api.weatherapi.com/v1"
