import os
import json
import requests
from math import ceil
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ---------- DB ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("Missing DB_* values in .env")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)

# ---------- ReccoBeats ----------
BASE_URL = "https://api.reccobeats.com"
AUDIO_FEATURES_URL = f"{BASE_URL}/v1/audio-features"

def spotify_id_from_href(href: str):
    if not href:
        return None
    parts = href.split("/track/")
    if len(parts) != 2:
        return None
    return parts[1].split("?")[0]

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

tracks_upsert = text("""
    INSERT INTO tracks (spotify_track_id, spotify_url, isrc, raw_json)
    VALUES (:spotify_track_id, :spotify_url, :isrc, CAST(:raw_json AS jsonb))
    ON CONFLICT (spotify_track_id)
    DO UPDATE SET
        spotify_url = COALESCE(EXCLUDED.spotify_url, tracks.spotify_url),
        isrc = COALESCE(EXCLUDED.isrc, tracks.isrc),
        raw_json = COALESCE(EXCLUDED.raw_json, tracks.raw_json)
""")

features_upsert = text("""
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

def main():
    # Pull distinct catalog track ids that are missing audio features
    with engine.connect() as conn:
        ids = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT c.spotify_track_id
            FROM catalog_tracks c
            LEFT JOIN audio_features a
              ON a.spotify_track_id = c.spotify_track_id
            WHERE c.spotify_track_id IS NOT NULL
              AND a.spotify_track_id IS NULL
            ORDER BY c.spotify_track_id
        """)).fetchall()]

    print("Catalog track IDs needing features:", len(ids))
    if not ids:
        print("✅ Nothing to enrich.")
        return

    BATCH_SIZE = 40
    batches = list(chunk_list(ids, BATCH_SIZE))
    print("Batches:", len(batches), f"(batch size {BATCH_SIZE})")

    total_returned = 0
    total_written = 0

    with engine.begin() as conn:
        for idx, batch in enumerate(batches, start=1):
            ids_param = ",".join(batch)

            resp = requests.get(
                AUDIO_FEATURES_URL,
                headers={"Accept": "application/json"},
                params={"ids": ids_param},
                timeout=30
            )

            if resp.status_code != 200:
                print(f"❌ Batch {idx}/{len(batches)} failed: {resp.status_code} {resp.text[:300]}")
                continue

            payload = resp.json()
            items = payload.get("content", [])
            total_returned += len(items)

            print(f"✅ Batch {idx}/{len(batches)} returned {len(items)} items")

            for it in items:
                href = it.get("href")
                spotify_track_id = spotify_id_from_href(href)
                if not spotify_track_id:
                    continue

                conn.execute(
                    tracks_upsert,
                    {
                        "spotify_track_id": spotify_track_id,
                        "spotify_url": href,
                        "isrc": it.get("isrc"),
                        "raw_json": json.dumps(it),
                    }
                )

                conn.execute(
                    features_upsert,
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
                    }
                )
                total_written += 1

    print("\n✅ Done enriching catalog candidates.")
    print("Total items returned by ReccoBeats:", total_returned)
    print("Total feature rows upserted:", total_written)

if __name__ == "__main__":
    main()
