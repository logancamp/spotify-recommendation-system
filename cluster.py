"""
cluster.py

Loads the current user's top tracks from postgres, runs kmeans clustering on their
audio features, then finds the best matching candidate tracks using cosine similarity.

The idea is that we group the user's taste into clusters (like "chill songs" vs "hype songs"),
then for each candidate we see which cluster it's closest to.

We also added weather context scoring - if it's rainy we bump up acoustic/low-energy tracks,
if it's hot we favor danceable stuff, etc. That part is controlled by beta in rerank_with_context().
"""

import os
import base64
import requests
import pandas as pd
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

from db_utils import read_df, write_ranked_recommendations, create_pipeline_run, complete_pipeline_run  # db helper functions we wrote

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# audio features we use to describe how a song sounds
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
    # check if a user hash was already injected (happens when streamlit calls this as a subprocess)
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
    Joins users -> user_top_tracks -> tracks -> audio_features tables.
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
    These are the songs we'll actually score and recommend.
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
    Returns an empty dict if nothing is in there yet.
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


# --- clustering functions ---

def cluster_tracks_by_audio(df: pd.DataFrame, n_clusters: int = 4, cluster_col: str = "audio_cluster"):
    """
    Run KMeans on the audio feature columns to group similar songs together.
    Returns the clustered df, a centroids df, and the kmeans model.
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

    cand_clean["assigned_cluster_id"] = [int(cent_clean.iloc[i]["cluster_id"]) for i in best_idx]
    cand_clean["cluster_similarity"] = best_score

    return cand_clean


# --- weather context scoring ---

def context_fit_score(row: pd.Series, context: dict) -> float:
    """
    Score how well a track fits the current weather.
    We basically made rules like:
      - cold/cloudy weather -> upbeat high energy songs
      - rainy/stormy -> calm acoustic tracks
      - hot -> danceable stuff
      - windy -> high energy
    Returns a float between 0 and 1.
    """
    if not context:
        return 0.0

    score = 0.0
    weight_sum = 0.0

    temp = context.get("temperature_c")
    desc = context.get("text_description", "")
    wind = context.get("wind_speed_m_s")

    energy     = float(row.get("energy",     0.5) or 0.5)
    valence    = float(row.get("valence",    0.5) or 0.5)
    dance      = float(row.get("danceability", 0.5) or 0.5)
    acoustic   = float(row.get("acousticness", 0.5) or 0.5)

    # cold or foggy outside = uplifting tracks feel better
    if (temp is not None and temp < 10) or any(w in desc for w in ["fog", "cloud", "overcast", "mist"]):
        score += (energy + valence) / 2
        weight_sum += 1.0

    # rainy/stormy = more chill acoustic vibes
    if any(w in desc for w in ["rain", "storm", "thunder", "drizzle", "shower"]):
        score += (acoustic + (1 - energy)) / 2
        weight_sum += 1.0

    # hot weather = danceable upbeat stuff
    if temp is not None and temp > 25:
        score += (dance + valence) / 2
        weight_sum += 1.0

    # windy = high energy tracks
    if wind is not None and wind > 10:
        score += energy
        weight_sum += 1.0

    if weight_sum == 0:
        return 0.5  # neutral: no strong weather signal, give mid score

    return score / weight_sum


def rerank_with_context(assigned_df: pd.DataFrame, context: dict, alpha: float = 1.0, beta: float = 0.0) -> pd.DataFrame:
    """
    Combine the cluster similarity score with the weather context score.
    final_score = alpha * cluster_similarity + beta * context_fit_score
    alpha=1, beta=0 means ignore weather and just use audio similarity.
    Deduplicates by track id and sorts best first.
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
        n_clusters=4,
        cluster_col="audio_cluster"
    )
    print("Cluster counts:")
    print(clustered_profile_df["audio_cluster"].value_counts().sort_index())

    # step 3: load candidate tracks (if empty, just use profile tracks for testing)
    candidates_df = load_candidates_from_db(user_hash=user_hash, limit=500)
    if len(candidates_df) == 0:
        print("catalog_tracks is empty for this user. Using profile tracks as demo candidates.")
        candidates_df = profile_df.copy()

    # step 4: assign each candidate to the nearest centroid
    assigned_candidates_df = assign_candidates_to_centroids(candidates_df, centroids_df)
    print("Assigned candidates rows:", len(assigned_candidates_df))
    print(assigned_candidates_df.head())

    # step 5: rerank using weather context if available
    # set APPLY_WEATHER=false to skip weather and use pure audio similarity
    apply_weather = os.getenv("APPLY_WEATHER", "true").lower() != "false"
    context = {}
    if apply_weather:
        context = load_latest_weather_context()
        if context:
            print(f"Weather context loaded: {context.get('text_description')}, "
                  f"{context.get('temperature_c')}°C")
        else:
            print("No weather context found — using audio-only ranking.")

    # if we have weather context blend it in (30% weather, 70% audio similarity)
    # otherwise just use audio similarity alone
    alpha = 0.7 if context else 1.0
    beta  = 0.3 if context else 0.0
    final_df = rerank_with_context(assigned_candidates_df, context=context, alpha=alpha, beta=beta)

    # keep the raw cluster_similarity score separate so the UI can reapply weather
    # client-side without being locked into the pipeline's alpha/beta choice
    if "cluster_similarity" not in final_df.columns:
        final_df["cluster_similarity"] = final_df["final_score"]

    # step 6: save outputs to data/ so we can inspect them
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

    # step 7: write the final ranked list to postgres so the frontend can read it
    user_row = read_df(f"SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}' LIMIT 1")
    if user_row.empty:
        print("WARNING: user not found in users table — skipping DB write for ranked_recommendations.")
    else:
        db_user_id = int(user_row.iloc[0]["user_id"])

        # create a pipeline run record so the UI can track history and detect new catalog tracks
        run_id = create_pipeline_run(user_id=db_user_id)
        print(f"Created pipeline run id={run_id}")

        n_written = write_ranked_recommendations(user_id=db_user_id, final_df=final_df, run_id=run_id)
        print(f"\n✅ Wrote {n_written} rows to ranked_recommendations (user_id={db_user_id})")

        complete_pipeline_run(run_id=run_id, catalog_added=len(candidates_df), recs_written=n_written)
        print(f"✅ Completed pipeline run id={run_id}")

    print("\nDone! To use weather/survey influence, make sure context_inputs has recent data")
    print("and adjust beta in rerank_with_context() above 0.")
