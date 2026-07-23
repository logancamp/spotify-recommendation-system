"""
inject_multi_user.py

Injects 5 fake users with distinct taste profiles, full top-track histories,
recently played events, daily snapshots, weather readings, and catalog entries.

All data uses the tracks already in the DB from inject_test_catalog.py.
Run inject_test_catalog.py first if you haven't already.

Safe to re-run (ON CONFLICT DO NOTHING / DO UPDATE throughout).

Usage:
    python inject_multi_user.py
"""

import os
import sys
import random
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from db_utils import get_engine

random.seed(42)

# ── Fake users with distinct sonic identities ─────────────────────────────────
# Each user has a spotify_user_hash, a display name, and a ranked list of
# INJ track IDs ordered by how much that user would love them.

USERS = [
    {
        "hash": "fake_user_rock_metal",
        "name": "Alex (Rock/Metal fan)",
        # High energy, low valence, high tempo — loves intense stuff
        "top_tracks": {
            "long_term":  ["INJ017","INJ022","INJ021","INJ020","INJ019","INJ014","INJ015","INJ016",
                           "INJ018","INJ088","INJ083","INJ082","INJ108","INJ109","INJ107","INJ106",
                           "INJ084","INJ087","INJ013","INJ071"],
            "medium_term":["INJ022","INJ021","INJ017","INJ020","INJ015","INJ016","INJ019","INJ014",
                           "INJ088","INJ018","INJ108","INJ109","INJ083","INJ082","INJ107","INJ084",
                           "INJ036","INJ037","INJ086","INJ085"],
            "short_term": ["INJ017","INJ022","INJ021","INJ108","INJ109","INJ015","INJ016","INJ020",
                           "INJ019","INJ088","INJ083","INJ014","INJ082","INJ018","INJ087","INJ106",
                           "INJ084","INJ085","INJ086","INJ036"],
        },
        "recently_played": ["INJ017","INJ022","INJ021","INJ020","INJ015","INJ016","INJ108","INJ019",
                            "INJ014","INJ088","INJ083","INJ109","INJ082","INJ018","INJ087","INJ107",
                            "INJ106","INJ084","INJ085","INJ086","INJ036","INJ037","INJ038","INJ039",
                            "INJ013","INJ017","INJ022","INJ021","INJ020","INJ108"],
        # Listens to heavy stuff on cold/cloudy days; energetic workout on clear days
        "weather_profile": [
            {"temp": 4,  "humidity": 78, "wind": 12, "desc": "cloudy"},
            {"temp": 8,  "humidity": 65, "wind": 18, "desc": "overcast"},
            {"temp": 15, "humidity": 45, "wind": 8,  "desc": "clear"},
            {"temp": 3,  "humidity": 82, "wind": 15, "desc": "fog"},
            {"temp": 12, "humidity": 55, "wind": 6,  "desc": "clear"},
            {"temp": 6,  "humidity": 72, "wind": 20, "desc": "wind"},
        ],
    },
    {
        "hash": "fake_user_classical_ambient",
        "name": "Maya (Classical/Ambient fan)",
        # High acousticness, high instrumentalness, low energy — peaceful listener
        "top_tracks": {
            "long_term":  ["INJ053","INJ054","INJ055","INJ056","INJ057","INJ058","INJ059","INJ060",
                           "INJ089","INJ090","INJ091","INJ092","INJ110","INJ111","INJ112","INJ048",
                           "INJ052","INJ051","INJ047","INJ033"],
            "medium_term":["INJ055","INJ053","INJ056","INJ054","INJ057","INJ089","INJ090","INJ110",
                           "INJ091","INJ058","INJ059","INJ092","INJ111","INJ112","INJ048","INJ047",
                           "INJ052","INJ051","INJ032","INJ033"],
            "short_term": ["INJ053","INJ055","INJ056","INJ110","INJ111","INJ057","INJ058","INJ054",
                           "INJ089","INJ090","INJ112","INJ091","INJ092","INJ059","INJ048","INJ047",
                           "INJ052","INJ051","INJ033","INJ034"],
        },
        "recently_played": ["INJ053","INJ055","INJ056","INJ110","INJ057","INJ054","INJ058","INJ089",
                            "INJ090","INJ111","INJ091","INJ092","INJ112","INJ059","INJ048","INJ047",
                            "INJ052","INJ051","INJ033","INJ034","INJ060","INJ053","INJ055","INJ056",
                            "INJ057","INJ089","INJ110","INJ111","INJ112","INJ053"],
        # Listens to calm stuff in rainy/misty conditions; still calm when sunny
        "weather_profile": [
            {"temp": 9,  "humidity": 88, "wind": 3,  "desc": "rain"},
            {"temp": 7,  "humidity": 92, "wind": 5,  "desc": "drizzle"},
            {"temp": 13, "humidity": 60, "wind": 4,  "desc": "mist"},
            {"temp": 11, "humidity": 75, "wind": 6,  "desc": "cloudy"},
            {"temp": 18, "humidity": 50, "wind": 5,  "desc": "clear"},
            {"temp": 8,  "humidity": 85, "wind": 4,  "desc": "rain"},
        ],
    },
    {
        "hash": "fake_user_pop_dance",
        "name": "Priya (Pop/Dance fan)",
        # High danceability, high valence, energetic — party listener
        "top_tracks": {
            "long_term":  ["INJ003","INJ004","INJ005","INJ001","INJ002","INJ006","INJ007","INJ008",
                           "INJ023","INJ024","INJ026","INJ028","INJ029","INJ030","INJ095","INJ096",
                           "INJ097","INJ098","INJ113","INJ120"],
            "medium_term":["INJ003","INJ004","INJ005","INJ023","INJ024","INJ001","INJ002","INJ006",
                           "INJ096","INJ097","INJ095","INJ098","INJ026","INJ028","INJ007","INJ008",
                           "INJ113","INJ120","INJ078","INJ079"],
            "short_term": ["INJ003","INJ004","INJ023","INJ024","INJ096","INJ097","INJ005","INJ001",
                           "INJ002","INJ095","INJ098","INJ006","INJ026","INJ028","INJ007","INJ008",
                           "INJ078","INJ079","INJ113","INJ120"],
        },
        "recently_played": ["INJ003","INJ004","INJ023","INJ024","INJ096","INJ097","INJ005","INJ001",
                            "INJ002","INJ095","INJ098","INJ006","INJ026","INJ028","INJ007","INJ008",
                            "INJ078","INJ079","INJ113","INJ120","INJ030","INJ025","INJ027","INJ009",
                            "INJ010","INJ003","INJ004","INJ023","INJ096","INJ024"],
        # Listens to dance music when hot and sunny; still dance in any weather
        "weather_profile": [
            {"temp": 28, "humidity": 55, "wind": 6,  "desc": "sunny"},
            {"temp": 31, "humidity": 48, "wind": 8,  "desc": "clear"},
            {"temp": 24, "humidity": 60, "wind": 10, "desc": "clear"},
            {"temp": 26, "humidity": 52, "wind": 7,  "desc": "sunny"},
            {"temp": 20, "humidity": 65, "wind": 9,  "desc": "partly cloudy"},
            {"temp": 29, "humidity": 45, "wind": 5,  "desc": "sunny"},
        ],
    },
    {
        "hash": "fake_user_hiphop_rb",
        "name": "Jordan (Hip-Hop/R&B fan)",
        # Speech-heavy, mid energy, strong beat — urban listener
        "top_tracks": {
            "long_term":  ["INJ067","INJ068","INJ069","INJ070","INJ071","INJ072","INJ073","INJ074",
                           "INJ075","INJ076","INJ077","INJ078","INJ079","INJ080","INJ001","INJ002",
                           "INJ009","INJ010","INJ011","INJ012"],
            "medium_term":["INJ067","INJ068","INJ069","INJ070","INJ071","INJ072","INJ075","INJ076",
                           "INJ073","INJ074","INJ077","INJ078","INJ079","INJ080","INJ002","INJ001",
                           "INJ009","INJ010","INJ011","INJ012"],
            "short_term": ["INJ067","INJ068","INJ069","INJ070","INJ072","INJ071","INJ073","INJ074",
                           "INJ075","INJ076","INJ077","INJ078","INJ079","INJ080","INJ001","INJ002",
                           "INJ009","INJ010","INJ011","INJ012"],
        },
        "recently_played": ["INJ067","INJ068","INJ069","INJ070","INJ072","INJ071","INJ073","INJ074",
                            "INJ075","INJ076","INJ077","INJ078","INJ079","INJ080","INJ001","INJ002",
                            "INJ009","INJ010","INJ011","INJ012","INJ067","INJ068","INJ069","INJ070",
                            "INJ072","INJ075","INJ076","INJ077","INJ001","INJ002"],
        "weather_profile": [
            {"temp": 18, "humidity": 62, "wind": 8,  "desc": "cloudy"},
            {"temp": 22, "humidity": 55, "wind": 10, "desc": "partly cloudy"},
            {"temp": 15, "humidity": 70, "wind": 12, "desc": "overcast"},
            {"temp": 20, "humidity": 58, "wind": 7,  "desc": "clear"},
            {"temp": 16, "humidity": 68, "wind": 9,  "desc": "cloudy"},
            {"temp": 23, "humidity": 52, "wind": 6,  "desc": "clear"},
        ],
    },
    {
        "hash": "fake_user_indie_folk",
        "name": "Sam (Indie/Folk fan)",
        # Acoustic, mid-energy, emotional — introspective listener
        "top_tracks": {
            "long_term":  ["INJ043","INJ044","INJ045","INJ046","INJ047","INJ048","INJ049","INJ050",
                           "INJ051","INJ052","INJ081","INJ082","INJ083","INJ084","INJ085","INJ086",
                           "INJ087","INJ088","INJ031","INJ032"],
            "medium_term":["INJ043","INJ044","INJ049","INJ050","INJ045","INJ046","INJ081","INJ082",
                           "INJ083","INJ047","INJ048","INJ084","INJ051","INJ052","INJ085","INJ086",
                           "INJ087","INJ088","INJ031","INJ033"],
            "short_term": ["INJ043","INJ044","INJ049","INJ050","INJ081","INJ082","INJ045","INJ046",
                           "INJ083","INJ047","INJ048","INJ084","INJ051","INJ052","INJ085","INJ086",
                           "INJ087","INJ088","INJ031","INJ032"],
        },
        "recently_played": ["INJ043","INJ044","INJ049","INJ050","INJ081","INJ082","INJ045","INJ046",
                            "INJ083","INJ047","INJ048","INJ084","INJ051","INJ052","INJ085","INJ086",
                            "INJ087","INJ088","INJ031","INJ032","INJ033","INJ037","INJ038","INJ039",
                            "INJ040","INJ043","INJ044","INJ049","INJ050","INJ081"],
        # Listens to acoustic on rainy/misty days; indie on mild clear days
        "weather_profile": [
            {"temp": 10, "humidity": 85, "wind": 5,  "desc": "rain"},
            {"temp": 12, "humidity": 80, "wind": 6,  "desc": "drizzle"},
            {"temp": 16, "humidity": 58, "wind": 8,  "desc": "clear"},
            {"temp": 9,  "humidity": 88, "wind": 4,  "desc": "mist"},
            {"temp": 14, "humidity": 62, "wind": 7,  "desc": "partly cloudy"},
            {"temp": 11, "humidity": 75, "wind": 5,  "desc": "cloudy"},
        ],
    },
]

# Snapshot dates: today, 3 days ago, 7 days ago — gives taste drift data
SNAPSHOT_DATES = [
    datetime.now(timezone.utc).date(),
    (datetime.now(timezone.utc) - timedelta(days=3)).date(),
    (datetime.now(timezone.utc) - timedelta(days=7)).date(),
]

# Weather readings: one every 6 hours for the past 7 days (28 readings)
# These get inserted into context_inputs and matched to recently played timestamps
def generate_weather_timeline():
    readings = []
    base = datetime.now(timezone.utc) - timedelta(days=7)
    conditions = [
        {"temp": 11, "humidity": 72, "wind": 8,  "desc": "clear"},
        {"temp": 9,  "humidity": 80, "wind": 12, "desc": "cloudy"},
        {"temp": 7,  "humidity": 88, "wind": 5,  "desc": "rain"},
        {"temp": 8,  "humidity": 85, "wind": 7,  "desc": "drizzle"},
        {"temp": 13, "humidity": 60, "wind": 9,  "desc": "partly cloudy"},
        {"temp": 15, "humidity": 52, "wind": 6,  "desc": "clear"},
        {"temp": 11, "humidity": 75, "wind": 14, "desc": "overcast"},
    ]
    for i in range(28):
        dt = base + timedelta(hours=i * 6)
        cond = conditions[i % len(conditions)]
        readings.append({"fetched_at": dt, **cond})
    return readings


def get_or_create_user(conn, user_hash):
    conn.execute(
        text("INSERT INTO users (spotify_user_hash) VALUES (:h) ON CONFLICT (spotify_user_hash) DO NOTHING"),
        {"h": user_hash}
    )
    return conn.execute(
        text("SELECT user_id FROM users WHERE spotify_user_hash = :h"),
        {"h": user_hash}
    ).scalar()


def main():
    engine = get_engine()

    print("Checking injected tracks exist...")
    with engine.connect() as conn: # type: ignore
        n = conn.execute(text("SELECT COUNT(*) FROM tracks WHERE spotify_track_id LIKE 'INJ%'")).scalar()
        if n < 100:
            print(f"❌ Only {n} injected tracks found. Run inject_test_catalog.py first.")
            sys.exit(1)
        print(f"✅ {n} injected tracks found in tracks table.")

    # ── Step 1: insert weather timeline into context_inputs ───────────────────
    weather_timeline = generate_weather_timeline()
    print(f"\nInserting {len(weather_timeline)} historical weather readings...")
    with engine.begin() as conn:
        for w in weather_timeline:
            conn.execute(text("""
                INSERT INTO context_inputs
                    (temperature_c, relative_humidity, wind_speed_m_s, text_description, fetched_at)
                VALUES
                    (:temp, :humidity, :wind, :desc, :fetched_at)
                ON CONFLICT DO NOTHING
            """), {
                "temp": w["temp"], "humidity": w["humidity"],
                "wind": w["wind"], "desc": w["desc"], "fetched_at": w["fetched_at"]
            })
    print(f"✅ Weather timeline inserted.")

    # ── Step 2: insert each fake user ─────────────────────────────────────────
    for user_spec in USERS:
        print(f"\n── {user_spec['name']} ({user_spec['hash']}) ──")

        with engine.begin() as conn:
            user_id = get_or_create_user(conn, user_spec["hash"])
            print(f"   user_id = {user_id}")

            # ── top tracks (3 time ranges) ────────────────────────────────────
            top_count = 0
            for time_range, track_ids in user_spec["top_tracks"].items():
                for rank, tid in enumerate(track_ids, start=1):
                    conn.execute(text("""
                        INSERT INTO user_top_tracks (user_id, spotify_track_id, rank, time_range, pulled_at)
                        VALUES (:uid, :tid, :rank, :tr, NOW())
                        ON CONFLICT (user_id, spotify_track_id, time_range)
                        DO UPDATE SET rank = EXCLUDED.rank, pulled_at = NOW()
                    """), {"uid": user_id, "tid": tid, "rank": rank, "tr": time_range})
                    top_count += 1
            print(f"   ✅ {top_count} top track rows")

            # ── top track snapshots (3 dates) ─────────────────────────────────
            snap_count = 0
            for snap_date in SNAPSHOT_DATES:
                for time_range, track_ids in user_spec["top_tracks"].items():
                    # Slightly shuffle ranks on older dates to simulate taste drift
                    shuffled = list(track_ids)
                    if snap_date < datetime.now(timezone.utc).date():
                        random.shuffle(shuffled)
                    for rank, tid in enumerate(shuffled, start=1):
                        conn.execute(text("""
                            INSERT INTO user_top_track_snapshots
                                (user_id, spotify_track_id, time_range, rank, snapshot_date, pulled_at)
                            VALUES (:uid, :tid, :tr, :rank, :snap_date, NOW())
                            ON CONFLICT (user_id, spotify_track_id, time_range, snapshot_date) DO NOTHING
                        """), {"uid": user_id, "tid": tid, "tr": time_range,
                               "rank": rank, "snap_date": snap_date})
                        snap_count += 1
            print(f"   ✅ {snap_count} snapshot rows across {len(SNAPSHOT_DATES)} dates")

            # ── recently played (30 events spread over past 7 days) ──────────
            recently_played = user_spec["recently_played"]
            rp_count = 0
            base_time = datetime.now(timezone.utc) - timedelta(days=7)
            # Spread 30 plays across 7 days — roughly every 5-6 hours
            for i, tid in enumerate(recently_played):
                # Add some jitter so plays don't all land on exact 6hr marks
                hours_offset = i * 5.6 + random.uniform(-1.5, 1.5)
                played_at = base_time + timedelta(hours=hours_offset)
                conn.execute(text("""
                    INSERT INTO user_recently_played (user_id, spotify_track_id, played_at, pulled_at)
                    VALUES (:uid, :tid, :played_at, NOW())
                    ON CONFLICT (user_id, spotify_track_id, played_at) DO NOTHING
                """), {"uid": user_id, "tid": tid, "played_at": played_at})
                rp_count += 1
            print(f"   ✅ {rp_count} recently played events")

            # ── catalog entries (all 120 injected tracks as candidates) ───────
            cat_count = 0
            all_inj = [f"INJ{str(i).zfill(3)}" for i in range(1, 121)]
            for tid in all_inj:
                conn.execute(text("""
                    INSERT INTO catalog_tracks
                        (user_id, spotify_track_id, source, query_used, seed_type, seed_value)
                    VALUES
                        (:uid, :tid, 'injected', 'multi_user_injection', 'injected', 'diverse_test_set')
                    ON CONFLICT (user_id, spotify_track_id, seed_type, seed_value) DO NOTHING
                """), {"uid": user_id, "tid": tid})
                cat_count += 1
            print(f"   ✅ {cat_count} catalog track entries")

            # ── pipeline run record ───────────────────────────────────────────
            run_id = conn.execute(text("""
                INSERT INTO pipeline_runs
                    (user_id, started_at, completed_at, catalog_tracks_added, recommendations_written)
                VALUES
                    (:uid, NOW() - INTERVAL '2 minutes', NOW(), :cat, 0)
                RETURNING id
            """), {"uid": user_id, "cat": cat_count}).scalar()
            print(f"   ✅ Pipeline run id={run_id}")

    # ── Step 3: summary ───────────────────────────────────────────────────────
    print("\n── Summary ──")
    with engine.connect() as conn:
        total_users  = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        total_rp     = conn.execute(text("SELECT COUNT(*) FROM user_recently_played")).scalar()
        total_snaps  = conn.execute(text("SELECT COUNT(*) FROM user_top_track_snapshots")).scalar()
        total_cat    = conn.execute(text("SELECT COUNT(*) FROM catalog_tracks")).scalar()
        total_runs   = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).scalar()
        total_wx     = conn.execute(text("SELECT COUNT(*) FROM context_inputs")).scalar()

    print(f"  Users:                {total_users}")
    print(f"  Pipeline runs:        {total_runs}")
    print(f"  Recently played:      {total_rp} events")
    print(f"  Top track snapshots:  {total_snaps} rows")
    print(f"  Catalog tracks:       {total_cat} entries")
    print(f"  Weather readings:     {total_wx}")
    print("\n✅ Done. Run the pipeline for each user to generate ranked recommendations.")
    print("   Or run cluster.py with SPOTIFY_USER_HASH set to each fake user hash.")


if __name__ == "__main__":
    main()