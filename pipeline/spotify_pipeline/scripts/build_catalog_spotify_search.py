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
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# ---------- DB ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("Missing Spotify or DB values in .env")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)


def get_access_token():
    """
    Get a spotify access token. If one was injected from streamlit, use that.
    Otherwise do the normal refresh token flow.
    """
    injected = os.getenv("SPOTIFY_ACCESS_TOKEN")
    if injected:
        return injected

    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_spotify_user(access_token: str) -> dict:
    url = "https://api.spotify.com/v1/me"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


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


def ensure_user(conn, spotify_user_hash: str):
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


def insert_catalog_track(conn, user_id, spotify_track_id, source, query_used, seed_type, seed_value, raw_json, pipeline_run_id=None):
    conn.execute(
        text("""
            INSERT INTO catalog_tracks (
                user_id,
                spotify_track_id,
                source,
                query_used,
                seed_type,
                seed_value,
                pipeline_run_id,
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
                :pipeline_run_id,
                NOW(),
                CAST(:raw_json AS jsonb)
            )
            ON CONFLICT (user_id, spotify_track_id, seed_type, seed_value)
            DO UPDATE SET pipeline_run_id = EXCLUDED.pipeline_run_id, pulled_at = NOW()
        """),
        {
            "user_id": user_id,
            "spotify_track_id": spotify_track_id,
            "source": source,
            "query_used": query_used,
            "seed_type": seed_type,
            "seed_value": seed_value,
            "pipeline_run_id": pipeline_run_id,
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


def derive_search_seeds_from_profile(conn, user_id: int, limit=10):
    """
    Build candidate-search seeds from the current user's profile.
    Uses top artists from long_term top tracks PLUS distinct artists from the
    last 7 days of recently played history, so the catalog stays fresh even when
    a user's "long_term" top tracks haven't changed much.
    """
    # --- long-term top artists ---
    rows = conn.execute(
        text("""
            SELECT
                t.primary_artist,
                COUNT(*) AS artist_count
            FROM user_top_tracks u
            JOIN tracks t
              ON t.spotify_track_id = u.spotify_track_id
            WHERE u.user_id = :user_id
              AND u.time_range = 'long_term'
              AND t.primary_artist IS NOT NULL
            GROUP BY t.primary_artist
            ORDER BY artist_count DESC, t.primary_artist
            LIMIT :limit
        """),
        {"user_id": user_id, "limit": limit}
    ).fetchall()

    seen_artists = set()
    seeds = []
    for r in rows:
        artist = r[0]
        if artist and artist not in seen_artists:
            seen_artists.add(artist)
            seeds.append({"seed_type": "artist", "seed_value": artist, "query_used": artist})

    # --- recently played artists (last 7 days, up to limit/2 extra) ---
    recent_rows = conn.execute(
        text("""
            SELECT DISTINCT t.primary_artist
            FROM user_recently_played rp
            JOIN tracks t ON t.spotify_track_id = rp.spotify_track_id
            WHERE rp.user_id = :user_id
              AND rp.played_at >= NOW() - INTERVAL '7 days'
              AND t.primary_artist IS NOT NULL
            ORDER BY t.primary_artist
            LIMIT :limit
        """),
        {"user_id": user_id, "limit": max(1, limit // 2)}
    ).fetchall()

    for r in recent_rows:
        artist = r[0]
        if artist and artist not in seen_artists:
            seen_artists.add(artist)
            seeds.append({"seed_type": "artist_recent", "seed_value": artist, "query_used": artist})

    return seeds


def main():
    access_token = get_access_token()
    total_attempted = 0

    current_user = get_current_spotify_user(access_token)
    spotify_user_id = current_user["id"]

    # cluster.py creates the run_id and injects it so all steps are stamped consistently
    run_id_raw = os.getenv("PIPELINE_RUN_ID")
    pipeline_run_id = int(run_id_raw) if run_id_raw else None

    print(f"Using Spotify user id for candidate pool: {spotify_user_id}")
    if pipeline_run_id:
        print(f"Stamping catalog tracks with pipeline_run_id={pipeline_run_id}")

    with engine.begin() as conn:
        user_id = ensure_user(conn, spotify_user_hash=spotify_user_id)

        search_seeds = derive_search_seeds_from_profile(conn, user_id=user_id, limit=10)

        if not search_seeds:
            raise SystemExit(
                f"No profile-derived search seeds found for user_id={user_id}. "
                "Run ingest_spotify_top_tracks.py first for this Spotify user."
            )

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
                    raw_json=track,
                    pipeline_run_id=pipeline_run_id,
                )
                total_attempted += 1

    print(f"✅ Done. Attempted {total_attempted} catalog_tracks inserts.")


if __name__ == "__main__":
    main()
