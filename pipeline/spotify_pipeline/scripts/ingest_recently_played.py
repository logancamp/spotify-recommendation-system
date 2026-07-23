"""
ingest_recently_played.py

Pulls the user's 50 most recently played tracks from Spotify and:
  1. Upserts them into the shared `tracks` table
  2. Inserts new rows into `user_recently_played` (duplicates silently ignored)
  3. Writes a daily snapshot of the current top tracks into `user_top_track_snapshots`
     so we can chart taste drift over time without losing historical rank data

Run after ingest_spotify_top_tracks.py — top tracks must already be in the DB
before we snapshot them.
"""

import os
import sys
import json
import base64
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# allow importing db_utils from the repo root when run as a subprocess
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from db_utils import write_recently_played

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([CLIENT_ID, CLIENT_SECRET, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("❌ Missing required env vars (Spotify/DB).")


def get_access_token() -> str:
    injected = os.getenv("SPOTIFY_ACCESS_TOKEN")
    if injected:
        return injected
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {auth_header}"},
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_user(access_token: str) -> dict:
    resp = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_recently_played(access_token: str, limit: int = 50) -> list[dict]:
    resp = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": limit},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json().get("items", [])


def init_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


def upsert_track(conn, track: dict):
    track_id = track.get("id")
    if not track_id:
        return
    artists = track.get("artists", [])
    primary_artist = artists[0].get("name") if artists else None
    conn.execute(
        text("""
            INSERT INTO tracks (
                spotify_track_id, name, primary_artist, popularity,
                duration_ms, explicit, spotify_url, isrc, raw_json
            )
            VALUES (
                :spotify_track_id, :name, :primary_artist, :popularity,
                :duration_ms, :explicit, :spotify_url, :isrc, CAST(:raw_json AS jsonb)
            )
            ON CONFLICT (spotify_track_id)
            DO UPDATE SET
                name            = COALESCE(EXCLUDED.name, tracks.name),
                primary_artist  = COALESCE(EXCLUDED.primary_artist, tracks.primary_artist),
                popularity      = COALESCE(EXCLUDED.popularity, tracks.popularity),
                duration_ms     = COALESCE(EXCLUDED.duration_ms, tracks.duration_ms),
                explicit        = COALESCE(EXCLUDED.explicit, tracks.explicit),
                spotify_url     = COALESCE(EXCLUDED.spotify_url, tracks.spotify_url),
                isrc            = COALESCE(EXCLUDED.isrc, tracks.isrc),
                raw_json        = EXCLUDED.raw_json
        """),
        {
            "spotify_track_id": track_id,
            "name": track.get("name"),
            "primary_artist": primary_artist,
            "popularity": track.get("popularity"),
            "duration_ms": track.get("duration_ms"),
            "explicit": track.get("explicit"),
            "spotify_url": track.get("external_urls", {}).get("spotify"),
            "isrc": track.get("external_ids", {}).get("isrc"),
            "raw_json": json.dumps(track),
        }
    )


def write_top_track_snapshots(conn, user_id: int):
    """
    Copy today's user_top_tracks rows into user_top_track_snapshots.
    Uses INSERT ... ON CONFLICT DO NOTHING so re-running on the same day is safe.
    """
    result = conn.execute(
        text("""
            INSERT INTO user_top_track_snapshots
                (user_id, spotify_track_id, time_range, rank, snapshot_date, pulled_at)
            SELECT
                user_id,
                spotify_track_id,
                time_range,
                rank,
                CURRENT_DATE,
                NOW()
            FROM user_top_tracks
            WHERE user_id = :user_id
            ON CONFLICT (user_id, spotify_track_id, time_range, snapshot_date)
            DO NOTHING
        """),
        {"user_id": user_id},
    )
    return result.rowcount


def get_or_create_user(conn, spotify_user_hash: str) -> int:
    conn.execute(
        text("INSERT INTO users (spotify_user_hash) VALUES (:h) ON CONFLICT (spotify_user_hash) DO NOTHING"),
        {"h": spotify_user_hash},
    )
    return conn.execute(
        text("SELECT user_id FROM users WHERE spotify_user_hash = :h"),
        {"h": spotify_user_hash},
    ).scalar()


def main():
    access_token = get_access_token()
    current_user = get_current_user(access_token)
    spotify_user_id = os.getenv("SPOTIFY_USER_HASH") or current_user["id"]
    print(f"Using Spotify user id: {spotify_user_id}")

    items = fetch_recently_played(access_token, limit=50)
    print(f"Fetched {len(items)} recently played items from Spotify.")

    engine = init_engine()
    with engine.begin() as conn:
        user_id = get_or_create_user(conn, spotify_user_id)

        # upsert the track rows first so the FK constraint on user_recently_played is satisfied
        for item in items:
            track = item.get("track", {})
            if track:
                upsert_track(conn, track)

        # snapshot today's top tracks while we're here
        snapped = write_top_track_snapshots(conn, user_id)
        print(f"✅ Wrote {snapped} new top-track snapshot rows for today.")

    # write recently played events (uses write_recently_played from db_utils)
    recently_played_rows = [
        {
            "spotify_track_id": item["track"]["id"],
            "played_at": item["played_at"],
        }
        for item in items
        if item.get("track") and item["track"].get("id")
    ]
    inserted = write_recently_played(user_id, recently_played_rows)
    print(f"✅ Inserted {inserted} new rows into user_recently_played (of {len(recently_played_rows)} fetched).")


if __name__ == "__main__":
    main()
