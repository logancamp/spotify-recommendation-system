"""
cluster.py (Strategy B, audio-only NOW; weather/survey LATER)

What this script does TODAY (audio-only):
- Reads the user's long_term top tracks + audio features directly from Postgres
  (tables: user_top_tracks, tracks, audio_features)
- Runs KMeans clustering on audio feature vectors
- Computes cluster centroids (mean feature vectors)
- (Optional) Loads candidate tracks from catalog_tracks if your team builds it later
- (Optional) Assigns candidates to the closest centroid using cosine similarity
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
       OR filter to clusters whose centroids match the context (high energy for "hype", etc.)
"""

import os
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

from db_utils import read_df  # Postgres -> DataFrame helper (Strategy B)

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
# 1) DB LOADERS
# ============================================================

def load_user_profile_from_db(time_range: str = "long_term") -> pd.DataFrame:
    """
    Loads the user's profile tracks + audio features from Postgres.

    Source tables:
      user_top_tracks JOIN tracks JOIN audio_features

    Returns columns needed for clustering plus metadata (name/artist).
    """
    sql = f"""
    SELECT
      u.rank,
      u.time_range,
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
    JOIN tracks t
      ON t.spotify_track_id = u.spotify_track_id
    JOIN audio_features a
      ON a.spotify_track_id = u.spotify_track_id
    WHERE u.time_range = '{time_range}'
    ORDER BY u.rank;
    """
    return read_df(sql)


def load_candidates_from_db(limit: int = 500) -> pd.DataFrame:
    """
    Loads candidate tracks from Postgres IF catalog_tracks is populated.

    Source tables:
      catalog_tracks JOIN tracks JOIN audio_features

    If your team hasn't built the catalog yet, this will likely return 0 rows.
    """
    sql = f"""
    SELECT
      c.catalog_id,
      c.source,
      c.query_used,
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
    JOIN tracks t
      ON t.spotify_track_id = c.spotify_track_id
    JOIN audio_features a
      ON a.spotify_track_id = c.spotify_track_id
    ORDER BY c.pulled_at DESC
    LIMIT {int(limit)};
    """
    return read_df(sql)


# ============================================================
# 2) DATA VALIDATION / CLEANING
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
# 3) CLUSTERING (AUDIO ONLY)
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
# 4) (FUTURE) CONTEXT SUPPORT: WEATHER + SURVEY
# ============================================================

def context_fit_score(row: pd.Series, context: dict) -> float:
    """
    FUTURE: Convert mood/weather context into a score for how well a track "fits".

    Example idea (simple):
      - If mood == 'hype', reward high energy and higher tempo.
      - If mood == 'calm', reward low energy and higher acousticness.
      - If weather == 'rain', reward lower valence, lower tempo (optional).

    Return a float score, e.g. between 0 and 1.

    For now: returns 0.0 (no effect).
    """
    return 0.0


def rerank_with_context(assigned_df: pd.DataFrame, context: dict, alpha: float = 1.0, beta: float = 0.0) -> pd.DataFrame:
    """
    FUTURE: Combine similarity score with context-based score.

    final_score = alpha * cluster_similarity + beta * context_fit_score

    For now beta=0.0 so it's purely audio similarity.
    """
    df = assigned_df.copy()
    if "cluster_similarity" not in df.columns:
        raise ValueError("assigned_df must include 'cluster_similarity' column.")
    df["context_score"] = df.apply(lambda r: context_fit_score(r, context), axis=1)
    df["final_score"] = alpha * df["cluster_similarity"] + beta * df["context_score"]
    return df.sort_values("final_score", ascending=False)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    """
    Default behavior: DB + audio-only.
    This matches your current Airflow pipeline outputs.
    """

    # 1) Load user profile from DB
    profile_df = load_user_profile_from_db(time_range="long_term")
    print("Loaded profile rows from Postgres:", len(profile_df))
    if profile_df.empty:
        raise SystemExit(
            "No profile rows returned. Run your Airflow DAG first to populate user_top_tracks + audio_features."
        )

    # 2) Cluster user profile tracks
    clustered_profile_df, centroids_df, _kmeans = cluster_tracks_by_audio(profile_df, n_clusters=4, cluster_col="audio_cluster")
    print("Cluster counts:")
    print(clustered_profile_df["audio_cluster"].value_counts().sort_index())

    # 3) Load candidates (if catalog exists); otherwise reuse profile for demo
    candidates_df = load_candidates_from_db(limit=500)
    if len(candidates_df) == 0:
        print("catalog_tracks is empty. Using profile tracks as demo candidates.")
        candidates_df = profile_df.copy()

    # 4) Assign candidates to closest centroid
    assigned_candidates_df = assign_candidates_to_centroids(candidates_df, centroids_df)
    print("Assigned candidates rows:", len(assigned_candidates_df))
    print(assigned_candidates_df.head())

    # 5) (Optional, future) rerank with context (beta=0 for now)
    context = {}  # later: load latest survey+weather from DB (context_inputs)
    final_df = rerank_with_context(assigned_candidates_df, context=context, alpha=1.0, beta=0.0)

    # 6) Save outputs for inspection/demo
    os.makedirs("data", exist_ok=True)
    clustered_profile_df.to_csv("data/profile_tracks_clustered_db.csv", index=False)
    centroids_df.to_csv("data/audio_cluster_centroids_db.csv", index=False)
    assigned_candidates_df.to_csv("data/candidates_assigned_db.csv", index=False)
    final_df.to_csv("data/candidates_ranked_db.csv", index=False)

    print("Saved outputs to data/:")
    print("- data/profile_tracks_clustered_db.csv")
    print("- data/audio_cluster_centroids_db.csv")
    print("- data/candidates_assigned_db.csv")
    print("- data/candidates_ranked_db.csv")

    print("\nNext (later): Add weather/survey by creating a context_inputs table + Streamlit UI,")
    print("then set beta>0 in rerank_with_context() to activate context-based ranking.")
