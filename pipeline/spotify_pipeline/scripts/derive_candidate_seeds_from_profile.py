import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise SystemExit("Missing DB_* values in .env")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)

def main():
    with engine.connect() as conn:
        rows = conn.execute(text("""
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
            LIMIT 10
        """)).fetchall()

    if not rows:
        raise SystemExit("No top artists found from user profile.")

    print("Derived artist seeds from user profile:")
    for r in rows:
        artist = r[0]
        count = r[1]
        print({
            "seed_type": "artist",
            "seed_value": artist,
            "query_used": artist,
            "artist_count": count
        })

if __name__ == "__main__":
    main()
