import pandas as pd
import requests

NWS_BASE = "https://api.weather.gov"
IPAPI_URL = "https://ipapi.co/json/"

HEADERS = {
    "User-Agent": "music-weather-enricher/1.0 (contact: your-email@example.com)",
    "Accept": "application/geo+json"
}

# This is the API call function with error handling and JSON parsing
def _safe_get_json(url, params=None, headers=None, timeout=60):
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _extract_value(field):
    # NWS API returns fields as {"value": actual_value, ...}, extract "value"
    if isinstance(field, dict):
        return field.get("value")
    return None

# Round timestamps to the nearest hour for matching with NWS observations
def _round_timestamp_to_hour(series):
    return pd.to_datetime(series, utc=True, errors="coerce").dt.floor("h")


def _get_current_ip_location(today_file=None):
    # First try to read from today_file if provided, otherwise fallback to IP API
    if today_file:
        df = pd.read_csv(today_file)
        lat = df["latitude"].iloc[0]
        lon = df["longitude"].iloc[0]
    else:
        data = _safe_get_json(IPAPI_URL, timeout=30)
        lat = data.get("latitude")
        lon = data.get("longitude")

    if lat is None or lon is None:
        raise ValueError("Could not determine location from IP")

    return {
        "latitude": float(lat),
        "longitude": float(lon),
    }

# Get the URL for nearby observation stations based on latitude and longitude
def _get_observation_stations_url(lat, lon):
    data = _safe_get_json(
        f"{NWS_BASE}/points/{lat},{lon}",
        headers=HEADERS
    )

    props = data.get("properties", {})
    stations_url = props.get("observationStations")

    if not stations_url:
        raise ValueError(f"No observation stations URL returned for point {lat}, {lon}")

    return stations_url

# Get the nearest station identifier from the list of nearby stations
def _get_nearest_station_identifier(lat, lon):
    stations_url = _get_observation_stations_url(lat, lon)
    data = _safe_get_json(stations_url, headers=HEADERS)

    features = data.get("features", [])
    if not features:
        raise ValueError(f"No nearby observation stations found for point {lat}, {lon}")

    station_props = features[0].get("properties", {})
    station_identifier = station_props.get("stationIdentifier")

    if not station_identifier:
        raise ValueError(f"Could not resolve station identifier for point {lat}, {lon}")

    return station_identifier

# Fetch observations for a given station and time range, return as DataFrame
def _fetch_station_observations(station_id, start_iso, end_iso):
    data = _safe_get_json(
        f"{NWS_BASE}/stations/{station_id}/observations",
        params={"start": start_iso, "end": end_iso},
        headers=HEADERS
    )

    # Compile rows of observations with relevant fields
    features = data.get("features", [])
    rows = []
    for feature in features:
        props = feature.get("properties", {})
        timestamp = props.get("timestamp")

        if not timestamp:
            continue

        rows.append({
            "station_id": station_id,
            "observation_time": pd.to_datetime(timestamp, utc=True, errors="coerce"),
            "temperature_c": _extract_value(props.get("temperature")),
            "dewpoint_c": _extract_value(props.get("dewpoint")),
            "wind_speed_m_s": _extract_value(props.get("windSpeed")),
            "wind_direction_deg": _extract_value(props.get("windDirection")),
            "barometric_pressure_pa": _extract_value(props.get("barometricPressure")),
            "visibility_m": _extract_value(props.get("visibility")),
            "relative_humidity": _extract_value(props.get("relativeHumidity")),
            "text_description": props.get("textDescription"),
        })

    return pd.DataFrame(rows)


# Given a DataFrame of observations and a target time, find the closest observation and return it as a Series
def _pick_closest_observation(observations_df, target_time):
    if observations_df.empty:
        return None

    temp_df = observations_df.copy()
    temp_df["time_diff_seconds"] = (
        temp_df["observation_time"] - target_time
    ).abs().dt.total_seconds()

    best_idx = temp_df["time_diff_seconds"].idxmin()
    return temp_df.loc[best_idx]


# Build a simple weather label based on text description, temperature, and humidity
def _build_weather_label(row):
    text_description = row.get("text_description")
    if isinstance(text_description, str) and text_description.strip():
        return text_description.strip().lower().replace(" ", "_")

    temp = row.get("temperature_c")
    humidity = row.get("relative_humidity")

    if pd.notna(temp):
        if temp <= 0:
            return "freezing"
        if temp < 10:
            return "cold"
        if temp < 22:
            return "mild"
        return "warm"

    if pd.notna(humidity) and humidity >= 85:
        return "humid"

    return "unknown"


def add_weather_to_recents_nws_ip(
    input_file=None,
    output_file=None,
    today_file=None,
    df=None,
    timestamp_col="played_at"
):
    # If df is not provided, read from input_file. If df is provided, ignore input_file.
    if df is None:
        if input_file is None:
            raise ValueError("input_file must be provided when df is None")
        df = pd.read_csv(input_file)

    df = df.copy()

    # Validate that the timestamp column exists
    if timestamp_col not in df.columns:
        raise ValueError(f"Missing required timestamp column: {timestamp_col}")

    # Round timestamps to the nearest hour for matching with NWS observations
    df["weather_hour"] = _round_timestamp_to_hour(df[timestamp_col])

    if df["weather_hour"].isna().any():
        bad_count = int(df["weather_hour"].isna().sum())
        raise ValueError(f"Could not parse {bad_count} timestamp values from column '{timestamp_col}'")

    location = _get_current_ip_location(today_file)
    latitude = location["latitude"]
    longitude = location["longitude"]

    # Add location info to the DataFrame for reference
    df["latitude"] = latitude
    df["longitude"] = longitude

    station_id = _get_nearest_station_identifier(latitude, longitude)
    df["station_id"] = station_id

    # Fetch observations for the station and time range, then enrich each row with the closest observation data
    start_time = df["weather_hour"].min() - pd.Timedelta(hours=2)
    end_time = df["weather_hour"].max() + pd.Timedelta(hours=2)
    obs_df = _fetch_station_observations(
        station_id,
        start_time.isoformat().replace("+00:00", "Z"),
        end_time.isoformat().replace("+00:00", "Z")
    )

    if obs_df.empty:
        raise ValueError("No NWS observation data was fetched for the provided rows.")

    # Enrich each row in the original DataFrame with the hourly weather data
    enriched_rows = []
    for _, row in df.iterrows():
        target_time = row["weather_hour"]
        closest = _pick_closest_observation(obs_df, target_time)

        row_dict = row.to_dict()

        if closest is not None:
            row_dict["observation_time"] = closest["observation_time"]
            row_dict["weather_time_diff_seconds"] = closest["time_diff_seconds"]
            row_dict["temperature_c"] = closest["temperature_c"]
            row_dict["dewpoint_c"] = closest["dewpoint_c"]
            row_dict["wind_speed_m_s"] = closest["wind_speed_m_s"]
            row_dict["wind_direction_deg"] = closest["wind_direction_deg"]
            row_dict["barometric_pressure_pa"] = closest["barometric_pressure_pa"]
            row_dict["visibility_m"] = closest["visibility_m"]
            row_dict["relative_humidity"] = closest["relative_humidity"]
            row_dict["text_description"] = closest["text_description"]
            row_dict["weather_label"] = _build_weather_label(closest)
        else:
            row_dict["observation_time"] = pd.NA
            row_dict["weather_time_diff_seconds"] = pd.NA
            row_dict["temperature_c"] = pd.NA
            row_dict["dewpoint_c"] = pd.NA
            row_dict["wind_speed_m_s"] = pd.NA
            row_dict["wind_direction_deg"] = pd.NA
            row_dict["barometric_pressure_pa"] = pd.NA
            row_dict["visibility_m"] = pd.NA
            row_dict["relative_humidity"] = pd.NA
            row_dict["text_description"] = pd.NA
            row_dict["weather_label"] = pd.NA

        enriched_rows.append(row_dict)

    enriched_df = pd.DataFrame(enriched_rows)

    # Optionally write the enriched DataFrame to a new CSV file
    if output_file is not None:
        enriched_df.to_csv(output_file, index=False)

    return enriched_df


if __name__ == "__main__":
    enriched_recents = add_weather_to_recents_nws_ip(
        input_file="data/recent_tracks_enriched.csv",
        today_file="data/today_weather.csv",
        output_file="data/recent_tracks_enriched_weather.csv",
        timestamp_col="played_at"
    )