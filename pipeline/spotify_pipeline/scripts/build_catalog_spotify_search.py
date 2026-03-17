import os
import json
import base64
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime, timezone

load_dotenv()

# ---------- Spotify ----------
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# ---------- DB ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([CLIENT_ID, CLIENT_SECRET, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("Missing Spotify or DB values in .env")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)


def get_access_token():
    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def spotify_search_tracks(access_token, query, limit=10):
    url = "https://api.spotify.com/v1/search"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "q": query,
            "type": "track",
            "limit": limit,
        },
        timeout=20,
    )

    if resp.status_code != 200:
        print("Search query failed:", query)
        print("Status:", resp.status_code)
        print("Body:", resp.text)
        resp.raise_for_status()

    return resp.json()

def ensure_demo_user(conn, spotify_user_hash="tdo18_demo"):
    """
    For now we keep the same prototype user pattern you already used.
    Later this should come from the real Spotify user identity.
    """
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

def upsert_track(conn, track):
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


def insert_catalog_track(conn, user_id, spotify_track_id, source, query_used, seed_type, seed_value, raw_json):
    conn.execute(
        text("""
            INSERT INTO catalog_tracks (
                user_id,
                spotify_track_id,
                source,
                query_used,
                seed_type,
                seed_value,
                pulled_at,
                raw_json
            )
            VALUES (
                :user_id,
                :spotify_track_id,
                :source,
                :query_used,
                :seed_type,
                :seed_value,
                NOW(),
                CAST(:raw_json AS jsonb)
            )
            ON CONFLICT (user_id, spotify_track_id, seed_type, seed_value)
            DO NOTHING
        """),
        {
            "user_id": user_id,
            "spotify_track_id": spotify_track_id,
            "source": source,
            "query_used": query_used,
            "seed_type": seed_type,
            "seed_value": seed_value,
            "raw_json": json.dumps(raw_json),
        }
    )


def save_raw_search_result(query, data):
    raw_dir = Path("data/raw_candidate_search")
    raw_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_query = query.lower().replace(" ", "_").replace("/", "_")
    out_file = raw_dir / f"{ts}_{safe_query}.json"

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw candidate search JSON: {out_file}")
    return str(out_file)

def derive_search_seeds_from_profile(conn, limit=10):
    """
    Build candidate-search seeds from the user's actual profile.
    For now, use top artist names from long_term tracks.
    """
    rows = conn.execute(
        text("""
            SELECT
                t.primary_artist,
                COUNT(*) AS artist_count
            FROM user_top_tracks u
            JOIN tracks t
              ON t.spotify_track_id = u.spotify_track_id
            WHERE u.time_range = 'long_term'
              AND t.primary_artist IS NOT NULL
            GROUP BY t.primary_artist
            ORDER BY artist_count DESC, t.primary_artist
            LIMIT :limit
        """),
        {"limit": limit}
    ).fetchall()

    seeds = []
    for r in rows:
        artist = r[0]
        seeds.append({
            "seed_type": "artist",
            "seed_value": artist,
            "query_used": artist
        })

    return seeds

def main():
    access_token = get_access_token()
    total_inserted = 0

    with engine.begin() as conn:
        user_id = ensure_demo_user(conn, spotify_user_hash="tdo18_demo")

        search_seeds = derive_search_seeds_from_profile(conn, limit=10)

        print("Using profile-derived candidate seeds:")
        for s in search_seeds:
            print(s)

        for seed in search_seeds:
            query = seed["query_used"]
            data = spotify_search_tracks(access_token, query, limit=10)
            items = data.get("tracks", {}).get("items", [])
            print(f"Query '{query}' returned {len(items)} tracks")
            save_raw_search_result(query, data)

            for track in items:
                upsert_track(conn, track)
                insert_catalog_track(
                    conn,
                    user_id=user_id,
                    spotify_track_id=track["id"],
                    source="spotify_search",
                    query_used=seed["query_used"],
                    seed_type=seed["seed_type"],
                    seed_value=seed["seed_value"],
                    raw_json=track
                )
                total_inserted += 1

    print(f"✅ Done. Inserted {total_inserted} catalog_tracks rows.")

if __name__ == "__main__":
    main()
