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
    with eng.connect() as conn: # type: ignore
        result = conn.execute(text(sql))
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


def create_pipeline_run(user_id: int) -> int:
    """
    Insert a new pipeline_runs row and return its id.
    Call this at the start of a pipeline run (in cluster.py main).
    """
    eng = get_engine()
    with eng.begin() as conn:
        run_id = conn.execute(
            text("INSERT INTO pipeline_runs (user_id) VALUES (:user_id) RETURNING id"),
            {"user_id": int(user_id)},
        ).scalar()
    return int(run_id)


def complete_pipeline_run(run_id: int, catalog_added: int, recs_written: int):
    """Mark a pipeline run as complete with summary counts."""
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text("""
                UPDATE pipeline_runs
                SET completed_at = NOW(),
                    catalog_tracks_added = :catalog_added,
                    recommendations_written = :recs_written
                WHERE id = :run_id
            """),
            {"run_id": run_id, "catalog_added": catalog_added, "recs_written": recs_written},
        )


def write_recently_played(user_id: int, tracks: list) -> int:
    """
    Insert recently played rows. Rows already present (same user/track/played_at)
    are silently ignored so we never duplicate events.
    Each dict must have keys: spotify_track_id, played_at.
    Returns the number of new rows inserted.
    """
    if not tracks:
        return 0
    eng = get_engine()
    rows = [
        {"user_id": int(user_id), "spotify_track_id": t["spotify_track_id"], "played_at": t["played_at"]}
        for t in tracks
    ]
    with eng.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO user_recently_played (user_id, spotify_track_id, played_at)
                VALUES (:user_id, :spotify_track_id, :played_at)
                ON CONFLICT (user_id, spotify_track_id, played_at) DO NOTHING
            """),
            rows,
        )
    return result.rowcount


def write_ranked_recommendations(user_id: int, final_df: "pd.DataFrame", run_id: int = None) -> int:
    """
    Save the ranked recommendations to postgres for a given user.
    Deletes the old recommendations first so we always have a fresh set.
    rank_position is 1-indexed (1 = best match).
    Stores cluster_similarity and context_score so the UI can show the
    breakdown and verify weather influence per song.
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
            "cluster_similarity": float(row.get("cluster_similarity", row["final_score"])),
            "context_score": float(row.get("context_score", 0.0)),
            "rank_position": rank_position,
            "pipeline_run_id": run_id,
        })

    with eng.begin() as conn:
        conn.execute(
            text("DELETE FROM ranked_recommendations WHERE user_id = :user_id"),
            {"user_id": int(user_id)},
        )
        if rows:
            conn.execute(
                text("""
                    INSERT INTO ranked_recommendations
                        (user_id, spotify_track_id, name, primary_artist,
                         final_score, cluster_similarity, context_score,
                         rank_position, pipeline_run_id)
                    VALUES
                        (:user_id, :spotify_track_id, :name, :primary_artist,
                         :final_score, :cluster_similarity, :context_score,
                         :rank_position, :pipeline_run_id)
                """),
                rows,
            )

    return len(rows)
