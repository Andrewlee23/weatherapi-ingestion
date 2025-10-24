# I dont have pg installed on my pc yet, and my laptop is in my bag so I'm going to test to csv first to see if weatherapi actually works 
import os
import csv
import requests
from dotenv import load_dotenv
from dateutil import parser as dtparse

load_dotenv()

API_KEY = os.getenv("WEATHERAPI_KEY")
BASE = "https://api.weatherapi.com/v1"

LOCATION = "Rochester,NY"  
FORECAST_DAYS = 3          


def get_forecast(location, days=3):
    url = f"{BASE}/forecast.json"
    params = {
        "key": API_KEY,
        "q": location,
        "days": days,
        "alerts": "yes",
        "aqi": "no"
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def write_csv(filename, row, mode="a"):
    file_exists = os.path.isfile(filename)
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists or mode == "w":
            writer.writeheader()
        writer.writerow(row)


def save_current_to_csv(data, location):
    current = data["current"]
    cond = current.get("condition", {})

    row = {
        "location": location,
        "obs_time": current.get("last_updated"),
        "temp_f": current.get("temp_f"),
        "condition_text": cond.get("text"),
        "condition_code": cond.get("code"),
        "wind_mph": current.get("wind_mph"),
        "gust_mph": current.get("gust_mph"),
        "precip_in": current.get("precip_in"),
        "cloud": current.get("cloud")
    }

    filename = f"{location.replace(',', '_')}_current.csv"
    write_csv(filename, row)


def save_forecast_to_csv(data, location):
    for daydata in data["forecast"]["forecastday"]:
        day = daydata["day"]
        cond = day.get("condition", {})
        row = {
            "location": location,
            "forecast_date": daydata["date"],
            "maxtemp_f": day.get("maxtemp_f"),
            "mintemp_f": day.get("mintemp_f"),
            "maxwind_mph": day.get("maxwind_mph"),
            "totalprecip_in": day.get("totalprecip_in"),
            "totalsnow_cm": day.get("totalsnow_cm"),
            "condition_text": cond.get("text"),
            "condition_code": cond.get("code"),
            "daily_chance_of_rain": day.get("daily_chance_of_rain"),
            "daily_chance_of_snow": day.get("daily_chance_of_snow")
        }
        filename = f"{location.replace(',', '_')}_forecast.csv"
        write_csv(filename, row)


def save_alerts_to_csv(data, location):
    alerts = data.get("alerts", {}).get("alert", [])
    if not alerts:
        return

    for a in alerts:
        row = {
            "location": location,
            "headline": a.get("headline"),
            "severity": a.get("severity"),
            "areas": a.get("areas"),
            "certainty": a.get("certainty"),
            "event": a.get("event"),
            "note": a.get("note"),
            "effective": a.get("effective"),
            "expires": a.get("expires")
        }
        filename = f"{location.replace(',', '_')}_alerts.csv"
        write_csv(filename, row)


def main():
    print(f"Fetching WeatherAPI data for {LOCATION}...")
    data = get_forecast(LOCATION, days=FORECAST_DAYS)

    save_current_to_csv(data, LOCATION)
    save_forecast_to_csv(data, LOCATION)
    save_alerts_to_csv(data, LOCATION)

    print("CSVs saved in the current directory")


if __name__ == "__main__":
    main()
