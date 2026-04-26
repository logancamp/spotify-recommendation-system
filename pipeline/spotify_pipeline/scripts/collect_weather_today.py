import requests
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Load .env from repo root (two levels up from this script)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"))

# Allow importing db_utils from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from sqlalchemy import text
from db_utils import get_engine

NWS_BASE = "https://api.weather.gov"

HEADERS = {
    "User-Agent": "music-weather/1.0 (contact: logan@example.com)",
    "Accept": "application/geo+json"
}

OUTPUT_FILE = "data/today_weather.csv"

# This is the API call function with error handling and JSON parsing
def safe_get_json(url, headers=None, params=None, timeout=20):
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_location():
    # try multiple ip providers in case of failure or rate limits
    providers = [
        "https://ipapi.co/json/",
        "http://ip-api.com/json/",
        "https://ipinfo.io/json"
    ]

    last_response = None

    # try each provider until we get a valid response with lat/lon
    for url in providers:
        try:
            data = safe_get_json(url, timeout=10) 
            last_response = data

            lat = data.get("latitude") or data.get("lat")
            lon = data.get("longitude") or data.get("lon")

            # ipinfo uses "loc": "lat,lon"
            if lat is None and lon is None and "loc" in data:
                parts = str(data["loc"]).split(",")
                if len(parts) == 2:
                    lat, lon = parts[0], parts[1]

            if lat is not None and lon is not None:
                return float(lat), float(lon)

        except Exception:
            continue

    raise ValueError(f"Location lookup failed from all providers. Last response: {last_response}")


def get_station(lat, lon):
    # Get nearest weather station for given lat/lon using NWS API
    point = safe_get_json(
        f"{NWS_BASE}/points/{lat},{lon}",
        headers=HEADERS
    )

    properties = point.get("properties", {})
    stations_url = properties.get("observationStations")

    if not stations_url:
        raise ValueError(f"No observation stations URL found for {lat}, {lon}")

    # Get stations and take the first one (closest)
    stations = safe_get_json(stations_url, headers=HEADERS)
    features = stations.get("features", [])

    if not features:
        raise ValueError(f"No stations returned for {lat}, {lon}")

    station_id = features[0].get("properties", {}).get("stationIdentifier")

    if not station_id:
        raise ValueError(f"Could not resolve station identifier for {lat}, {lon}")

    return station_id


def extract_value(field):
    # NWS API returns fields as {"value": actual_value, ...}, extract "value"
    if isinstance(field, dict):
        return field.get("value")
    return None


def get_current_weather(station):
    # Get latest observation for the station and extract relevant fields
    obs = safe_get_json(
        f"{NWS_BASE}/stations/{station}/observations/latest",
        headers=HEADERS
    )

    props = obs.get("properties", {})
    timestamp = props.get("timestamp")
    text_description = props.get("textDescription")

    # Extract some weather data features and handle missing fields for data
    return {
        "station_id": station,
        "observation_time": timestamp,
        "temperature_c": extract_value(props.get("temperature")),
        "relative_humidity": extract_value(props.get("relativeHumidity")),
        "wind_speed_m_s": extract_value(props.get("windSpeed")),
        "dewpoint_c": extract_value(props.get("dewpoint")),
        "visibility_m": extract_value(props.get("visibility")),
        "text_description": text_description,
    }


if __name__ == "__main__":
    lat, lon = get_location()
    station = get_station(lat, lon)
    weather = get_current_weather(station)

    weather["latitude"] = lat
    weather["longitude"] = lon

    # 1) Save to CSV (repo root data/ folder)
    repo_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
    output_file = os.path.join(repo_root, "data", "today_weather.csv")
    df = pd.DataFrame([weather])
    df.to_csv(output_file, index=False)
    print(f"Saved today's weather to {output_file}")

    # 2) Write to context_inputs table in Postgres
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO context_inputs (
                    temperature_c, relative_humidity, wind_speed_m_s,
                    text_description, latitude, longitude, observation_time
                ) VALUES (
                    :temperature_c, :relative_humidity, :wind_speed_m_s,
                    :text_description, :latitude, :longitude, :observation_time
                )
            """),
            {
                "temperature_c": weather.get("temperature_c"),
                "relative_humidity": weather.get("relative_humidity"),
                "wind_speed_m_s": weather.get("wind_speed_m_s"),
                "text_description": weather.get("text_description"),
                "latitude": weather.get("latitude"),
                "longitude": weather.get("longitude"),
                "observation_time": weather.get("observation_time"),
            }
        )
    print("✅ Wrote weather to context_inputs table in Postgres")