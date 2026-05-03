"""
cluster.py

Loads the current user's top tracks from postgres, runs kmeans clustering on their
audio features, then finds the best matching candidate tracks using cosine similarity.

The idea is that we group the user's taste into clusters (like "chill songs" vs "hype songs"),
then for each candidate we see which cluster it's closest to.

When APPLY_WEATHER=true we use a learned weather approach instead of hardcoded rules:
  1. Join recently played tracks to historical weather readings by timestamp
  2. Cluster those plays by weather features (temp, humidity, wind, description flags)
  3. Find which weather cluster today's conditions fall into
  4. Use the average audio profile of tracks played in that weather cluster as a
     secondary centroid — candidates close to that audio profile get a boost
  5. final_score = 0.7 * cluster_similarity + 0.3 * weather_audio_similarity

This means the weather influence is personalised — it reflects what YOU actually listen
to in different conditions rather than generic assumptions.

Falls back to audio-only ranking if there isn't enough recently played + weather history
to build meaningful weather clusters (need at least 2 plays with matched weather readings).
"""

import os
import base64
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler

from db_utils import read_df, write_ranked_recommendations, create_pipeline_run, complete_pipeline_run

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

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

# weather features we encode for clustering
WEATHER_FEATURE_COLS = [
    "temperature_c",
    "relative_humidity",
    "wind_speed_m_s",
    "is_rainy",
    "is_cloudy",
    "is_sunny",
]


# --- spotify auth helpers ---

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
    injected = os.getenv("SPOTIFY_USER_HASH")
    if injected:
        print(f"Using injected SPOTIFY_USER_HASH: {injected}")
        return injected
    user = get_current_spotify_user()
    spotify_user_id = user["id"]
    print(f"Using Spotify user id in cluster.py: {spotify_user_id}")
    return spotify_user_id


# --- db loading functions ---

def load_user_profile_from_db(user_hash: str, time_range: str = "long_term") -> pd.DataFrame:
    """
    Pull the user's top tracks and audio features from postgres.
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
    Pull candidate tracks for this user from the catalog_tracks table.
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


def load_latest_weather_context() -> dict:
    """
    Grab the most recent weather entry from the context_inputs table.
    """
    sql = """
        SELECT temperature_c, relative_humidity, wind_speed_m_s, text_description
        FROM context_inputs
        ORDER BY fetched_at DESC
        LIMIT 1;
    """
    try:
        df = read_df(sql)
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "temperature_c": row.get("temperature_c"),
            "relative_humidity": row.get("relative_humidity"),
            "wind_speed_m_s": row.get("wind_speed_m_s"),
            "text_description": str(row.get("text_description") or "").lower(),
        }
    except Exception as e:
        print(f"WARNING: Could not load weather context: {e}")
        return {}


def load_recently_played_with_weather(user_hash: str) -> pd.DataFrame:
    """
    Join recently played tracks to the nearest weather reading by timestamp.
    We match each play to the closest context_inputs row in time (within 3 hours).
    Returns rows with both audio features and weather features for each play.
    """
    sql = f"""
    SELECT
        rp.played_at,
        t.spotify_track_id,
        a.acousticness, a.danceability, a.energy, a.instrumentalness,
        a.liveness, a.loudness, a.speechiness, a.tempo, a.valence,
        cx.temperature_c, cx.relative_humidity, cx.wind_speed_m_s,
        cx.text_description
    FROM user_recently_played rp
    JOIN users u ON u.user_id = rp.user_id
    JOIN tracks t ON t.spotify_track_id = rp.spotify_track_id
    JOIN audio_features a ON a.spotify_track_id = rp.spotify_track_id
    JOIN LATERAL (
        SELECT temperature_c, relative_humidity, wind_speed_m_s, text_description
        FROM context_inputs
        WHERE ABS(EXTRACT(EPOCH FROM (fetched_at - rp.played_at))) < 10800
        ORDER BY ABS(EXTRACT(EPOCH FROM (fetched_at - rp.played_at)))
        LIMIT 1
    ) cx ON true
    WHERE u.spotify_user_hash = '{user_hash}';
    """
    try:
        return read_df(sql)
    except Exception as e:
        print(f"WARNING: Could not load recently played with weather: {e}")
        return pd.DataFrame()


# --- data cleaning helpers ---

def validate_audio_cols(df: pd.DataFrame) -> None:
    missing = [c for c in AUDIO_FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required audio feature columns: {missing}")


def coerce_numeric_audio(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in AUDIO_FEATURE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def encode_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn raw weather columns into a numeric feature vector for clustering.
    Adds is_rainy, is_cloudy, is_sunny flags from text_description.
    """
    df = df.copy()
    desc = df["text_description"].fillna("").str.lower()
    df["is_rainy"]  = desc.str.contains("rain|storm|drizzle|shower|thunder").astype(float)
    df["is_cloudy"] = desc.str.contains("cloud|overcast|fog|mist").astype(float)
    df["is_sunny"]  = desc.str.contains("clear|sunny|fair").astype(float)
    for col in ["temperature_c", "relative_humidity", "wind_speed_m_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(df[col].median() if col in df else 0)
    return df


# --- clustering functions ---

def select_optimal_k(X: pd.DataFrame, k_min: int = 2, k_max: int = 8) -> int:
    """
    Sweep k from k_min to k_max and return the k that maximises the silhouette score.
    Silhouette score measures intra-cluster cohesion vs inter-cluster separation (-1 to 1,
    higher is better). Falls back to k_min if not enough distinct points exist.
    """
    from sklearn.metrics import silhouette_score

    distinct_points = len(X.drop_duplicates())
    k_max = min(k_max, len(X) - 1, distinct_points - 1)
    k_min = max(2, k_min)

    if k_max < k_min:
        print(f"Not enough distinct points for silhouette sweep — using k={k_min}")
        return k_min

    best_k, best_score = k_min, -1.0
    for k in range(k_min, k_max + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        # silhouette_score requires at least 2 distinct labels
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(X, labels)
        print(f"  k={k}  silhouette={score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k

    print(f"Optimal k={best_k} (silhouette={best_score:.4f})")
    return best_k


def cluster_tracks_by_audio(df: pd.DataFrame, n_clusters: int = None, cluster_col: str = "audio_cluster"): # type: ignore
    """
    Run KMeans on the audio feature columns to group similar songs together.
    If n_clusters is None, automatically selects the optimal k via silhouette score sweep
    (k=2 through k=8). Returns the clustered df, a centroids df, and the kmeans model.
    """
    df = df.copy()
    validate_audio_cols(df)
    df = coerce_numeric_audio(df)

    clean_df = df.dropna(subset=AUDIO_FEATURE_COLS).copy()
    if clean_df.empty:
        raise ValueError("No rows remain after dropping missing audio feature values.")

    X = clean_df[AUDIO_FEATURE_COLS]

    if n_clusters is None:
        print("Running silhouette sweep to find optimal k...")
        k = select_optimal_k(X, k_min=2, k_max=8)
    else:
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
    For each candidate track, find which cluster centroid it's most similar to.
    Uses cosine similarity. Adds assigned_cluster_id and cluster_similarity columns.
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

    cand_clean["assigned_cluster_id"] = [int(cent_clean.iloc[i]["cluster_id"] or 0) for i in best_idx]
    cand_clean["cluster_similarity"] = best_score

    return cand_clean


# --- learned weather clustering ---

def build_weather_audio_centroid(user_hash: str, today_context: dict) -> pd.Series | None:
    """
    The proper weather influence approach:

    1. Load recently played tracks joined to historical weather readings
    2. Cluster those plays by weather features (temp, humidity, rain flags etc.)
    3. Find which weather cluster today's conditions fall into
    4. Return the average audio feature vector of tracks played in that weather cluster

    This centroid represents "what this user actually listens to in this kind of weather"
    and is used to score candidates — songs close to it get a weather boost.

    Returns None if there isn't enough data to build meaningful clusters
    (need at least 5 plays with matched weather readings).
    """
    plays_df = load_recently_played_with_weather(user_hash)

    if plays_df.empty:
        print("Weather clustering: no recently played + weather matches found. Falling back to audio-only.")
        return None

    plays_df = encode_weather_features(plays_df)
    plays_df = coerce_numeric_audio(plays_df)
    plays_df = plays_df.dropna(subset=AUDIO_FEATURE_COLS + WEATHER_FEATURE_COLS)

    if len(plays_df) < 5:
        print(f"Weather clustering: only {len(plays_df)} matched plays, need 5+. Falling back.")
        return None

    # cluster the plays by weather features
    scaler = StandardScaler()
    W = scaler.fit_transform(plays_df[WEATHER_FEATURE_COLS])
    n_weather_clusters = min(4, len(plays_df) // 2)
    n_weather_clusters = max(2, n_weather_clusters)

    kmeans_weather = KMeans(n_clusters=n_weather_clusters, random_state=42, n_init=10)
    plays_df["weather_cluster"] = kmeans_weather.fit_predict(W)

    # encode today's weather the same way and find its nearest cluster
    today_row = pd.DataFrame([{
        "temperature_c":    today_context.get("temperature_c", 15),
        "relative_humidity": today_context.get("relative_humidity", 60),
        "wind_speed_m_s":   today_context.get("wind_speed_m_s", 5),
        "text_description": today_context.get("text_description", ""),
    }])
    today_row = encode_weather_features(today_row)
    today_row[WEATHER_FEATURE_COLS] = today_row[WEATHER_FEATURE_COLS].fillna(0.0)
    today_scaled = scaler.transform(today_row[WEATHER_FEATURE_COLS])
    today_cluster = int(kmeans_weather.predict(today_scaled)[0])

    # get the average audio profile of tracks played in that weather cluster
    cluster_plays = plays_df[plays_df["weather_cluster"] == today_cluster]
    if cluster_plays.empty:
        print("Weather clustering: today's weather cluster has no plays. Falling back.")
        return None

    audio_centroid = cluster_plays[AUDIO_FEATURE_COLS].mean()

    print(f"Weather clustering: today matched weather cluster {today_cluster} "
          f"({len(cluster_plays)} historical plays)")
    print(f"  Weather audio centroid — energy={audio_centroid['energy']:.2f}, "
          f"valence={audio_centroid['valence']:.2f}, "
          f"acousticness={audio_centroid['acousticness']:.2f}")

    return audio_centroid


def score_candidates_by_weather_centroid(
    assigned_df: pd.DataFrame,
    weather_audio_centroid: pd.Series,
) -> pd.DataFrame:
    """
    Score each candidate by cosine similarity to the weather audio centroid.
    Adds a context_score column (0-1).
    """
    df = assigned_df.copy()
    df = coerce_numeric_audio(df)
    clean = df.dropna(subset=AUDIO_FEATURE_COLS).copy()

    centroid_vec = weather_audio_centroid[AUDIO_FEATURE_COLS].values.reshape(1, -1) # type: ignore
    sims = cosine_similarity(clean[AUDIO_FEATURE_COLS].values, centroid_vec).flatten()
    clean["context_score"] = sims

    return clean


def rerank_with_context(
    assigned_df: pd.DataFrame,
    context_score_col: str = "context_score",
    alpha: float = 1.0,
    beta: float = 0.0,
) -> pd.DataFrame:
    """
    Combine cluster_similarity with context_score into final_score.
    final_score = alpha * cluster_similarity + beta * context_score
    alpha=1, beta=0 = audio-only.
    """
    df = assigned_df.copy()

    if "cluster_similarity" not in df.columns:
        raise ValueError("assigned_df must include 'cluster_similarity' column.")
    if "spotify_track_id" not in df.columns:
        raise ValueError("assigned_df must include 'spotify_track_id' column.")

    if context_score_col not in df.columns:
        df[context_score_col] = 0.0

    df["final_score"] = alpha * df["cluster_similarity"] + beta * df[context_score_col]
    df = df.sort_values("final_score", ascending=False).copy()
    df = df.drop_duplicates(subset=["spotify_track_id"], keep="first").copy()

    return df


# --- main ---

if __name__ == "__main__":
    user_hash = get_current_user_hash()

    # step 1: load the user's top tracks from db
    profile_df = load_user_profile_from_db(user_hash=user_hash, time_range="long_term")
    print("Loaded profile rows from Postgres:", len(profile_df))
    if profile_df.empty:
        raise SystemExit(
            "No profile rows returned for the current Spotify user. "
            "Run ingest_spotify_top_tracks.py and enrich_audio_features first."
        )

    # step 2: cluster those tracks by audio features
    clustered_profile_df, centroids_df, _kmeans = cluster_tracks_by_audio(
        profile_df,
        n_clusters=None,  # auto-select via silhouette sweep # type: ignore
        cluster_col="audio_cluster"
    )
    print(f"Optimal k={len(centroids_df)} clusters selected via silhouette score")
    print("Cluster counts:")
    print(clustered_profile_df["audio_cluster"].value_counts().sort_index())

    # step 3: load candidate tracks
    candidates_df = load_candidates_from_db(user_hash=user_hash, limit=500)
    if len(candidates_df) == 0:
        print("catalog_tracks is empty for this user. Using profile tracks as demo candidates.")
        candidates_df = profile_df.copy()

    # step 4: assign each candidate to the nearest audio centroid
    assigned_candidates_df = assign_candidates_to_centroids(candidates_df, centroids_df)
    print("Assigned candidates rows:", len(assigned_candidates_df))

    # step 5: weather influence using learned weather clusters (only if APPLY_WEATHER=true)
    apply_weather = os.getenv("APPLY_WEATHER", "true").lower() != "false"
    weather_centroid = None

    if apply_weather:
        today_context = load_latest_weather_context()
        if today_context:
            print(f"Today's weather: {today_context.get('text_description')}, "
                  f"{today_context.get('temperature_c')}°C")
            weather_centroid = build_weather_audio_centroid(user_hash, today_context)
        else:
            print("No weather context found — using audio-only ranking.")

    if weather_centroid is not None:
        # score candidates by similarity to the learned weather audio profile
        assigned_candidates_df = score_candidates_by_weather_centroid(
            assigned_candidates_df, weather_centroid
        )
        alpha, beta = 0.7, 0.3
        print("Using learned weather clustering (alpha=0.7, beta=0.3)")
    else:
        assigned_candidates_df["context_score"] = 0.0
        alpha, beta = 1.0, 0.0
        print("Using audio-only ranking (no weather influence)")

    final_df = rerank_with_context(assigned_candidates_df, alpha=alpha, beta=beta)

    if "cluster_similarity" not in final_df.columns:
        final_df["cluster_similarity"] = final_df["final_score"]

    # step 6: save outputs to data/
    os.makedirs("data", exist_ok=True)
    safe_user = user_hash.replace("/", "_")
    clustered_profile_df.to_csv(f"data/{safe_user}_profile_tracks_clustered_db.csv", index=False)
    centroids_df.to_csv(f"data/{safe_user}_audio_cluster_centroids_db.csv", index=False)
    assigned_candidates_df.to_csv(f"data/{safe_user}_candidates_assigned_db.csv", index=False)
    final_df.to_csv(f"data/{safe_user}_candidates_ranked_db.csv", index=False)
    print("Saved outputs to data/")

    # step 7: write to postgres
    user_row = read_df(f"SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}' LIMIT 1")
    if user_row.empty:
        print("WARNING: user not found in users table — skipping DB write.")
    else:
        db_user_id = int(user_row.iloc[0]["user_id"])
        run_id = create_pipeline_run(user_id=db_user_id)
        print(f"Created pipeline run id={run_id}")
        n_written = write_ranked_recommendations(user_id=db_user_id, final_df=final_df, run_id=run_id)
        print(f"\n✅ Wrote {n_written} rows to ranked_recommendations (user_id={db_user_id})")
        complete_pipeline_run(run_id=run_id, catalog_added=len(candidates_df), recs_written=n_written)
        print(f"✅ Completed pipeline run id={run_id}")

    print("\nDone!")