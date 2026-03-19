"""
cluster.py (Strategy B, audio-only NOW; weather/survey LATER)

What this script does TODAY (audio-only):
- Gets the current Spotify user from /v1/me using the refresh token
- Reads THAT user's long_term top tracks + audio features directly from Postgres
  (tables: users, user_top_tracks, tracks, audio_features)
- Runs KMeans clustering on audio feature vectors
- Loads THAT user's candidate tracks from catalog_tracks
- Assigns candidates to the closest centroid using cosine similarity
- Deduplicates final ranked output by spotify_track_id
- Saves results to data/ for inspection

How you'll add weather + survey later (high-level):
- Add a new table (e.g., context_inputs) to store "mood/survey + weather" signals.
- Have Streamlit write survey responses into context_inputs.
- Have a weather fetch script write current weather into context_inputs.
- Then in this script:
    1) read the latest context_inputs row for the user
    2) convert mood/weather into a "target preference vector" or weighting rules
    3) re-rank candidates using:
          final_score = alpha * cosine_to_centroid + beta * context_fit_score
       OR filter to clusters whose centroids match the context
"""

import os
import base64
import requests
import pandas as pd
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

from db_utils import read_df  # Postgres -> DataFrame helper

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# -----------------------------
# Audio features used to describe how a song sounds
# -----------------------------
AUDIO_FEATURE_COLS = [
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "liveness",
    "loudness",
    "speechiness",
    "tempo",
    "valence",
]


# ============================================================
# 1) SPOTIFY USER HELPERS
# ============================================================

def get_spotify_access_token():
    if not all([SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN]):
        raise SystemExit("Missing Spotify OAuth values in .env")

    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()

    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": SPOTIFY_REFRESH_TOKEN,
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_spotify_user():
    access_token = get_spotify_access_token()
    resp = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def get_current_user_hash():
    user = get_current_spotify_user()
    spotify_user_id = user["id"]
    print(f"Using Spotify user id in cluster.py: {spotify_user_id}")
    return spotify_user_id


# ============================================================
# 2) DB LOADERS
# ============================================================

def load_user_profile_from_db(user_hash: str, time_range: str = "long_term") -> pd.DataFrame:
    """
    Loads the CURRENT user's profile tracks + audio features from Postgres.

    Source tables:
      users -> user_top_tracks -> tracks -> audio_features
    """
    sql = f"""
    SELECT
      u.rank,
      u.time_range,
      usr.spotify_user_hash,
      t.spotify_track_id,
      t.name,
      t.primary_artist,
      a.acousticness,
      a.danceability,
      a.energy,
      a.instrumentalness,
      a.liveness,
      a.loudness,
      a.speechiness,
      a.tempo,
      a.valence
    FROM user_top_tracks u
    JOIN users usr
      ON usr.user_id = u.user_id
    JOIN tracks t
      ON t.spotify_track_id = u.spotify_track_id
    JOIN audio_features a
      ON a.spotify_track_id = u.spotify_track_id
    WHERE usr.spotify_user_hash = '{user_hash}'
      AND u.time_range = '{time_range}'
    ORDER BY u.rank;
    """
    return read_df(sql)


def load_candidates_from_db(user_hash: str, limit: int = 500) -> pd.DataFrame:
    """
    Loads candidate tracks for the CURRENT user from Postgres.

    Source tables:
      users -> catalog_tracks -> tracks -> audio_features
    """
    sql = f"""
    SELECT
      c.catalog_id,
      c.source,
      c.query_used,
      c.seed_type,
      c.seed_value,
      usr.spotify_user_hash,
      t.spotify_track_id,
      t.name,
      t.primary_artist,
      a.acousticness,
      a.danceability,
      a.energy,
      a.instrumentalness,
      a.liveness,
      a.loudness,
      a.speechiness,
      a.tempo,
      a.valence
    FROM catalog_tracks c
    JOIN users usr
      ON usr.user_id = c.user_id
    JOIN tracks t
      ON t.spotify_track_id = c.spotify_track_id
    JOIN audio_features a
      ON a.spotify_track_id = c.spotify_track_id
    WHERE usr.spotify_user_hash = '{user_hash}'
    ORDER BY c.pulled_at DESC
    LIMIT {int(limit)};
    """
    return read_df(sql)


# ============================================================
# 3) DATA VALIDATION / CLEANING
# ============================================================

def validate_audio_cols(df: pd.DataFrame) -> None:
    missing = [c for c in AUDIO_FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required audio feature columns: {missing}")


def coerce_numeric_audio(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in AUDIO_FEATURE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ============================================================
# 4) CLUSTERING (AUDIO ONLY)
# ============================================================

def cluster_tracks_by_audio(df: pd.DataFrame, n_clusters: int = 4, cluster_col: str = "audio_cluster"):
    """
    Run KMeans clustering on AUDIO_FEATURE_COLS.

    Returns:
      clustered_df: df + cluster assignments
      centroids_df: per-cluster average feature vector
      kmeans model
    """
    df = df.copy()
    validate_audio_cols(df)
    df = coerce_numeric_audio(df)

    clean_df = df.dropna(subset=AUDIO_FEATURE_COLS).copy()
    if clean_df.empty:
        raise ValueError("No rows remain after dropping missing audio feature values.")

    X = clean_df[AUDIO_FEATURE_COLS]
    distinct_points = len(X.drop_duplicates())
    k = min(n_clusters, len(clean_df), distinct_points)
    k = max(1, k)

    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    clean_df[cluster_col] = kmeans.fit_predict(X)

    centroids_df = (
        clean_df.groupby(cluster_col)[AUDIO_FEATURE_COLS]
        .mean()
        .reset_index()
        .rename(columns={cluster_col: "cluster_id"})
    )

    return clean_df, centroids_df, kmeans


def assign_candidates_to_centroids(candidates_df: pd.DataFrame, centroids_df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign each candidate track to the most similar centroid using cosine similarity.

    Output columns added:
      assigned_cluster_id
      cluster_similarity
    """
    candidates_df = candidates_df.copy()
    centroids_df = centroids_df.copy()

    validate_audio_cols(candidates_df)
    validate_audio_cols(centroids_df)

    candidates_df = coerce_numeric_audio(candidates_df)
    centroids_df = coerce_numeric_audio(centroids_df)

    cand_clean = candidates_df.dropna(subset=AUDIO_FEATURE_COLS).copy()
    cent_clean = centroids_df.dropna(subset=AUDIO_FEATURE_COLS).copy()

    if cand_clean.empty:
        raise ValueError("No candidate rows remain after dropping missing audio feature values.")
    if cent_clean.empty:
        raise ValueError("No centroid rows remain after dropping missing audio feature values.")

    sim = cosine_similarity(cand_clean[AUDIO_FEATURE_COLS], cent_clean[AUDIO_FEATURE_COLS])
    best_idx = sim.argmax(axis=1)
    best_score = sim.max(axis=1)

    cand_clean["assigned_cluster_id"] = [int(cent_clean.iloc[i]["cluster_id"]) for i in best_idx]
    cand_clean["cluster_similarity"] = best_score

    return cand_clean


# ============================================================
# 5) (FUTURE) CONTEXT SUPPORT: WEATHER + SURVEY
# ============================================================

def context_fit_score(row: pd.Series, context: dict) -> float:
    """
    FUTURE: Convert mood/weather context into a score for how well a track fits.
    For now: returns 0.0 (no effect).
    """
    return 0.0


def rerank_with_context(assigned_df: pd.DataFrame, context: dict, alpha: float = 1.0, beta: float = 0.0) -> pd.DataFrame:
    """
    Combine similarity score with optional context-based score.

    final_score = alpha * cluster_similarity + beta * context_fit_score

    Important:
    - Sort highest score first
    - Keep only the best-scoring row per spotify_track_id
    """
    df = assigned_df.copy()

    if "cluster_similarity" not in df.columns:
        raise ValueError("assigned_df must include 'cluster_similarity' column.")

    if "spotify_track_id" not in df.columns:
        raise ValueError("assigned_df must include 'spotify_track_id' column.")

    df["context_score"] = df.apply(lambda r: context_fit_score(r, context), axis=1)
    df["final_score"] = alpha * df["cluster_similarity"] + beta * df["context_score"]

    df = df.sort_values("final_score", ascending=False).copy()
    df = df.drop_duplicates(subset=["spotify_track_id"], keep="first").copy()

    return df


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    """
    Default behavior: DB + audio-only + CURRENT USER.
    """

    user_hash = get_current_user_hash()

    # 1) Load current user's profile from DB
    profile_df = load_user_profile_from_db(user_hash=user_hash, time_range="long_term")
    print("Loaded profile rows from Postgres:", len(profile_df))
    if profile_df.empty:
        raise SystemExit(
            "No profile rows returned for the current Spotify user. "
            "Run ingest_spotify_top_tracks.py and enrich_audio_features first."
        )

    # 2) Cluster current user's profile tracks
    clustered_profile_df, centroids_df, _kmeans = cluster_tracks_by_audio(
        profile_df,
        n_clusters=4,
        cluster_col="audio_cluster"
    )
    print("Cluster counts:")
    print(clustered_profile_df["audio_cluster"].value_counts().sort_index())

    # 3) Load current user's candidates; if empty, reuse profile for demo
    candidates_df = load_candidates_from_db(user_hash=user_hash, limit=500)
    if len(candidates_df) == 0:
        print("catalog_tracks is empty for this user. Using profile tracks as demo candidates.")
        candidates_df = profile_df.copy()

    # 4) Assign candidates to closest centroid
    assigned_candidates_df = assign_candidates_to_centroids(candidates_df, centroids_df)
    print("Assigned candidates rows:", len(assigned_candidates_df))
    print(assigned_candidates_df.head())

    # 5) (Future) rerank with context
    context = {}  # later: load latest survey+weather from DB (context_inputs)
    final_df = rerank_with_context(assigned_candidates_df, context=context, alpha=1.0, beta=0.0)

    # 6) Save outputs for inspection/demo (per-user filenames)
    os.makedirs("data", exist_ok=True)

    safe_user = user_hash.replace("/", "_")

    profile_out = f"data/{safe_user}_profile_tracks_clustered_db.csv"
    centroids_out = f"data/{safe_user}_audio_cluster_centroids_db.csv"
    assigned_out = f"data/{safe_user}_candidates_assigned_db.csv"
    ranked_out = f"data/{safe_user}_candidates_ranked_db.csv"

    clustered_profile_df.to_csv(profile_out, index=False)
    centroids_df.to_csv(centroids_out, index=False)
    assigned_candidates_df.to_csv(assigned_out, index=False)
    final_df.to_csv(ranked_out, index=False)

    print("Saved outputs to data/:")
    print(f"- {profile_out}")
    print(f"- {centroids_out}")
    print(f"- {assigned_out}")
    print(f"- {ranked_out}")

    print("\nNext (later): Add weather/survey by creating a context_inputs table + Streamlit UI,")
    print("then set beta>0 in rerank_with_context() to activate context-based ranking.")
