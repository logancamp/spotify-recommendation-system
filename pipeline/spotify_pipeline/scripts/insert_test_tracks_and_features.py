# ~/airflow/spotify_pipeline/scripts/insert_test_tracks_and_features.py
#
# Purpose:
# - Fetch audio features for multiple Spotify track IDs via ReccoBeats bulk endpoint:
#     GET https://api.reccobeats.com/v1/audio-features?ids=<comma-separated spotify ids>
# - Upsert into Postgres tables:
#     tracks (keyed by spotify_track_id)
#     audio_features (keyed by spotify_track_id)
#
# Requirements:
# - .env contains DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
# - pip install requests python-dotenv sqlalchemy psycopg2-binary
#
# Run:
#   python scripts/insert_test_tracks_and_features.py

import os
import json
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ---------- DB connection ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("Missing one or more DB_* values in .env")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)

# ---------- ReccoBeats bulk audio features ----------
BASE_URL = "https://api.reccobeats.com"
URL = f"{BASE_URL}/v1/audio-features"

# Use the three Spotify IDs you tested successfully
spotify_ids = [
    "18vXApRmJSgQ6wG2ll9AOg",  # RAPSTAR — Polo G
    "7ytR5pFWmSjzHJIeQkgog4",  # ROCKSTAR — DaBaby
    "30UFKKWSOC2Xr6KfWcyvsI",  # ROCKSTAR — DaBaby (alt)
]

ids_param = ",".join(spotify_ids)

resp = requests.get(
    URL,
    headers={"Accept": "application/json"},
    params={"ids": ids_param},
    timeout=20,
)
resp.raise_for_status()

payload = resp.json()
items = payload.get("content", [])

print("✅ ReccoBeats returned items:", len(items))

def spotify_id_from_href(href: str) -> str | None:
    """
    href example: https://open.spotify.com/track/<spotify_id>
    """
    if not href:
        return None
    parts = href.split("/track/")
    if len(parts) != 2:
        return None
    return parts[1].split("?")[0]

# ---------- Upsert into Postgres ----------
TRACKS_UPSERT_SQL = text("""
    INSERT INTO tracks (
        spotify_track_id,
        name,
        primary_artist,
        spotify_url,
        isrc,
        raw_json
    )
    VALUES (
        :spotify_track_id,
        :name,
        :primary_artist,
        :spotify_url,
        :isrc,
        CAST(:raw_json AS jsonb)
    )
    ON CONFLICT (spotify_track_id)
    DO UPDATE SET
        spotify_url = EXCLUDED.spotify_url,
        isrc = COALESCE(EXCLUDED.isrc, tracks.isrc),
        raw_json = EXCLUDED.raw_json
""")

AUDIO_FEATURES_UPSERT_SQL = text("""
    INSERT INTO audio_features (
        spotify_track_id,
        reccobeats_track_uuid,
        href,
        isrc,
        acousticness,
        danceability,
        energy,
        instrumentalness,
        key,
        liveness,
        loudness,
        mode,
        speechiness,
        tempo,
        valence,
        source,
        pulled_at,
        raw_json
    )
    VALUES (
        :spotify_track_id,
        :reccobeats_track_uuid,
        :href,
        :isrc,
        :acousticness,
        :danceability,
        :energy,
        :instrumentalness,
        :key,
        :liveness,
        :loudness,
        :mode,
        :speechiness,
        :tempo,
        :valence,
        'reccobeats',
        NOW(),
        CAST(:raw_json AS jsonb)
    )
    ON CONFLICT (spotify_track_id)
    DO UPDATE SET
        reccobeats_track_uuid = EXCLUDED.reccobeats_track_uuid,
        href = EXCLUDED.href,
        isrc = COALESCE(EXCLUDED.isrc, audio_features.isrc),
        acousticness = EXCLUDED.acousticness,
        danceability = EXCLUDED.danceability,
        energy = EXCLUDED.energy,
        instrumentalness = EXCLUDED.instrumentalness,
        key = EXCLUDED.key,
        liveness = EXCLUDED.liveness,
        loudness = EXCLUDED.loudness,
        mode = EXCLUDED.mode,
        speechiness = EXCLUDED.speechiness,
        tempo = EXCLUDED.tempo,
        valence = EXCLUDED.valence,
        pulled_at = NOW(),
        raw_json = EXCLUDED.raw_json
""")

rows_written = 0
with engine.begin() as conn:
    for it in items:
        href = it.get("href")
        spotify_track_id = spotify_id_from_href(href)

        if not spotify_track_id:
            print("⚠️ Skipping item missing spotify id from href:", it)
            continue

        # Upsert minimal track record (we'll fill name/artist later from Spotify API ingestion)
        conn.execute(
            TRACKS_UPSERT_SQL,
            {
                "spotify_track_id": spotify_track_id,
                "name": None,
                "primary_artist": None,
                "spotify_url": href,
                "isrc": it.get("isrc"),
                "raw_json": json.dumps(it),
            },
        )

        # Upsert features
        conn.execute(
            AUDIO_FEATURES_UPSERT_SQL,
            {
                "spotify_track_id": spotify_track_id,
                "reccobeats_track_uuid": it.get("id"),
                "href": href,
                "isrc": it.get("isrc"),
                "acousticness": it.get("acousticness"),
                "danceability": it.get("danceability"),
                "energy": it.get("energy"),
                "instrumentalness": it.get("instrumentalness"),
                "key": it.get("key"),
                "liveness": it.get("liveness"),
                "loudness": it.get("loudness"),
                "mode": it.get("mode"),
                "speechiness": it.get("speechiness"),
                "tempo": it.get("tempo"),
                "valence": it.get("valence"),
                "raw_json": json.dumps(it),
            },
        )

        rows_written += 1

print(f"✅ Inserted/updated {rows_written} track(s) and audio_features row(s).")
