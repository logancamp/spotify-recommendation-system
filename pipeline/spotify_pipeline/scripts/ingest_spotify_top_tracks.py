import os
import json
import base64
import requests
from datetime import datetime
from dotenv import load_dotenv
import boto3
from sqlalchemy import create_engine, text

load_dotenv()

# ---------- Spotify ----------
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# ---------- AWS ----------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# ---------- DB ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

required = [CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN, AWS_REGION, S3_BUCKET,
            DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]
if not all(required):
    raise SystemExit("❌ Missing required env vars in .env (Spotify/AWS/DB).")

def get_spotify_access_token() -> str:
    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        token_url,
        headers={"Authorization": f"Basic {auth_header}"},
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def spotify_get_top_tracks(access_token: str, time_range: str, limit: int = 50):
    url = "https://api.spotify.com/v1/me/top/tracks"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"time_range": time_range, "limit": limit},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()

def init_s3_client():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    return session.client("s3")

def upload_json_to_s3(s3, bucket: str, key: str, obj: dict):
    body = json.dumps(obj, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

def init_db_engine():
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_pre_ping=True)

def upsert_track(conn, track):
    # Minimal normalization (we can enrich later)
    track_id = track["id"]
    name = track.get("name")
    popularity = track.get("popularity")
    duration_ms = track.get("duration_ms")
    explicit = track.get("explicit")
    spotify_url = track.get("external_urls", {}).get("spotify")
    isrc = track.get("external_ids", {}).get("isrc")

    primary_artist = None
    if track.get("artists"):
        primary_artist = track["artists"][0].get("name")

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
                name = COALESCE(EXCLUDED.name, tracks.name),
                primary_artist = COALESCE(EXCLUDED.primary_artist, tracks.primary_artist),
                popularity = COALESCE(EXCLUDED.popularity, tracks.popularity),
                duration_ms = COALESCE(EXCLUDED.duration_ms, tracks.duration_ms),
                explicit = COALESCE(EXCLUDED.explicit, tracks.explicit),
                spotify_url = COALESCE(EXCLUDED.spotify_url, tracks.spotify_url),
                isrc = COALESCE(EXCLUDED.isrc, tracks.isrc),
                raw_json = EXCLUDED.raw_json
        """),
        {
            "spotify_track_id": track_id,
            "name": name,
            "primary_artist": primary_artist,
            "popularity": popularity,
            "duration_ms": duration_ms,
            "explicit": explicit,
            "spotify_url": spotify_url,
            "isrc": isrc,
            "raw_json": json.dumps(track),
        }
    )

def ensure_user(conn, spotify_user_hash: str = "demo_user") -> int:
    # For prototype: store hashed label; later can hash actual spotify user id.
    conn.execute(
        text("""
            INSERT INTO users (spotify_user_hash)
            VALUES (:h)
            ON CONFLICT (spotify_user_hash) DO NOTHING
        """),
        {"h": spotify_user_hash}
    )
    user_id = conn.execute(
        text("SELECT user_id FROM users WHERE spotify_user_hash = :h"),
        {"h": spotify_user_hash}
    ).scalar()
    return user_id

def insert_user_top_track(conn, user_id: int, track_id: str, rank: int, time_range: str):
    conn.execute(
        text("""
            INSERT INTO user_top_tracks (user_id, spotify_track_id, rank, time_range, pulled_at)
            VALUES (:user_id, :spotify_track_id, :rank, :time_range, NOW())
        """),
        {"user_id": user_id, "spotify_track_id": track_id, "rank": rank, "time_range": time_range}
    )

def main():
    access_token = get_spotify_access_token()
    s3 = init_s3_client()
    engine = init_db_engine()

    pulled_at = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    time_ranges = ["short_term", "medium_term", "long_term"]

    with engine.begin() as conn:
        user_id = ensure_user(conn, spotify_user_hash="tdo18_demo")

        for tr in time_ranges:
            data = spotify_get_top_tracks(access_token, tr, limit=50)

            # 1) Save raw to S3
            key = f"raw/spotify/top_tracks/tdo18_demo/{tr}/top_tracks_{pulled_at}.json"
            upload_json_to_s3(s3, S3_BUCKET, key, data)
            print(f"✅ Uploaded raw Spotify top tracks to s3://{S3_BUCKET}/{key}")

            # 2) Normalize into DB
            items = data.get("items", [])
            for idx, track in enumerate(items, start=1):
                upsert_track(conn, track)
                insert_user_top_track(conn, user_id, track["id"], rank=idx, time_range=tr)

            print(f"✅ Inserted {len(items)} top tracks into DB for time_range={tr}")

    print("✅ Done: Spotify top tracks ingested (raw to S3 + normalized to Postgres).")

if __name__ == "__main__":
    main()
