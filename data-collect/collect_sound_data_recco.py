import os
import time
import pandas as pd
import requests

BASE_URL = "https://api.reccobeats.com/v1"
REQUEST_DELAY_SECONDS = 0.5
RETRY_DELAY_SECONDS = 2.0
MAX_RETRIES = 5

INPUT_FILE = "data/saved_tracks.csv"
OUTPUT_FILE = "data/saved_tracks_enriched.csv"

# INPUT_FILE = "data/recent_tracks.csv"
# OUTPUT_FILE = "data/recent_tracks_enriched.csv"

# INPUT_FILE = "data/top_tracks.csv"
# OUTPUT_FILE = "data/top_tracks_enriched.csv"

START_INDEX = 0
BATCH_SIZE = len(pd.read_csv(INPUT_FILE))
API_CHUNK_SIZE = 40


def build_headers():
    headers = {
        "Accept": "application/json"
    }
    return headers


def get_json(url, params=None):
    retries = 0
    while True:
        # API call with timeout and error handling
        response = requests.get(
            url,
            headers=build_headers(),
            params=params,
            timeout=30
        )

        if response.status_code == 429:
            wait_seconds = float(response.headers.get("Retry-After", RETRY_DELAY_SECONDS))
            print(f"429 hit. Waiting {wait_seconds} seconds...")
            time.sleep(wait_seconds)
            retries += 1

            if retries > MAX_RETRIES:
                print("Max retries exceeded.")
                return None

            continue

        if response.status_code != 200:
            print(f"Request failed: {response.status_code} {response.text}")
            return None

        try:
            return response.json()
        except Exception:
            print("Failed to parse JSON response.")
            return None


# Extract Spotify ID from item, checking both 'spotifyId' field and 'href' URL
def extract_spotify_id(item):
    if not isinstance(item, dict):
        return None

    if item.get("spotifyId"):
        return item["spotifyId"]

    href = item.get("href")
    if isinstance(href, str) and "/track/" in href:
        return href.rstrip("/").split("/")[-1]

    return None

# Extract ISRC from item, checking 'isrc' field
def extract_isrc(item):
    if not isinstance(item, dict):
        return None

    return item.get("isrc")


def fetch_features_for_ids(id_list):
    feature_map = {}

    # Process IDs in chunks to respect API limits
    for chunk_start in range(0, len(id_list), API_CHUNK_SIZE):
        chunk_ids = [x for x in id_list[chunk_start:chunk_start + API_CHUNK_SIZE] if pd.notna(x) and str(x).strip()]

        if not chunk_ids:
            continue

        print(f"Fetching audio features for IDs {chunk_start} to {chunk_start + len(chunk_ids) - 1}")

        # API call to fetch features for this chunk of IDs
        response_data = get_json(
            f"{BASE_URL}/audio-features",
            params={"ids": ",".join(str(x) for x in chunk_ids)}
        )

        if response_data is None:
            print("No data returned for this chunk.")
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        
        # Fetch features and build mapping for both Spotify ID and ISRC
        features_list = response_data["content"]
        for item in features_list:
            spotify_id = extract_spotify_id(item)
            isrc = extract_isrc(item)

            if spotify_id:
                feature_map[("spotify", spotify_id)] = item

            if isrc:
                feature_map[("isrc", isrc)] = item

        time.sleep(REQUEST_DELAY_SECONDS)

    return feature_map


if __name__ == "__main__":
    df = pd.read_csv(INPUT_FILE)
    batch = df.iloc[START_INDEX:START_INDEX + BATCH_SIZE].copy()

    # Basic validation
    if batch.empty:
        print("No rows in this batch.")
        raise SystemExit

    print("Processing rows:", batch.index.min(), "to", batch.index.max())
    spotify_ids = batch["track_id"].dropna().astype(str).tolist()
    isrc_ids = []

    if "isrc" in batch.columns:
        isrc_ids = batch["isrc"].dropna().astype(str).tolist()

    # Try Spotify IDs first
    spotify_feature_map = fetch_features_for_ids(spotify_ids)

    # Fallback: try ISRC only for tracks not found by Spotify ID
    missing_isrcs = []
    for _, row in batch.iterrows():
        spotify_id = str(row["track_id"])
        if ("spotify", spotify_id) not in spotify_feature_map:
            isrc_value = row.get("isrc")
            if pd.notna(isrc_value) and str(isrc_value).strip():
                missing_isrcs.append(str(isrc_value).strip())

    isrc_feature_map = fetch_features_for_ids(missing_isrcs)
    results = []

    # Combine results and prepare output
    for i, row in batch.iterrows():
        spotify_id = str(row["track_id"])
        isrc_value = row.get("isrc")
        isrc_value = str(isrc_value).strip() if pd.notna(isrc_value) else None

        print(f"Processing {i}: {row['track_name']} - {row['artist_names']}")

        result = row.to_dict()
        result["features_found"] = 0
        result["match_source"] = None

        features = None

        # First try to find features by Spotify ID, then fallback to ISRC if not found
        if ("spotify", spotify_id) in spotify_feature_map:
            features = spotify_feature_map[("spotify", spotify_id)]
            result["features_found"] = 1
            result["match_source"] = "spotify_id"

        elif isrc_value and ("isrc", isrc_value) in isrc_feature_map:
            features = isrc_feature_map[("isrc", isrc_value)]
            result["features_found"] = 1
            result["match_source"] = "isrc"
        
        # If features were found, add them to the result; otherwise, log that no features were found
        if features:
            result.update(features)
        else:
            print(f"  No audio features found for track_id={spotify_id} isrc={isrc_value}")
        results.append(result)

    # Save results to CSV
    out_df = pd.DataFrame(results)
    file_exists = os.path.exists(OUTPUT_FILE)

    if not out_df.empty:
        out_df.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not file_exists,
            index=False
        )

    print("Saved:", OUTPUT_FILE)
    time.sleep(REQUEST_DELAY_SECONDS)
