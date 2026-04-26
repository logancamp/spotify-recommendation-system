import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# load env vars from .env file
load_dotenv()

def get_engine():
    """
    Build a SQLAlchemy engine from the DB_* env vars in .env.
    Throws a clear error if any required values are missing.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": pwd
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing DB values in .env: {missing}")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True)

def read_df(sql: str):
    """
    Run a SQL query and return the results as a pandas DataFrame.
    We use the manual fetch approach instead of pd.read_sql() because
    pd.read_sql() had some issues with our sqlalchemy version.
    """
    eng = get_engine()
    with eng.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


def write_ranked_recommendations(user_id: int, final_df: "pd.DataFrame") -> int:
    """
    Save the ranked recommendations to postgres for a given user.
    Deletes the old recommendations first so we always have a fresh set.
    rank_position is 1-indexed (1 = best match).
    Returns how many rows were written.
    """
    eng = get_engine()

    rows = []
    for rank_position, (_, row) in enumerate(
        final_df.sort_values("final_score", ascending=False).iterrows(), start=1
    ):
        rows.append({
            "user_id": int(user_id),
            "spotify_track_id": row["spotify_track_id"],
            "name": row.get("name"),
            "primary_artist": row.get("primary_artist"),
            "final_score": float(row["final_score"]),
            "rank_position": rank_position,
        })

    with eng.begin() as conn:
        # wipe out old recommendations for this user before inserting new ones
        conn.execute(
            text("DELETE FROM ranked_recommendations WHERE user_id = :user_id"),
            {"user_id": int(user_id)},
        )
        if rows:
            conn.execute(
                text("""
                    INSERT INTO ranked_recommendations
                        (user_id, spotify_track_id, name, primary_artist, final_score, rank_position)
                    VALUES
                        (:user_id, :spotify_track_id, :name, :primary_artist, :final_score, :rank_position)
                """),
                rows,
            )

    return len(rows)
