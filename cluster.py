import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

# audio features used to describe how a song sounds
AUDIO_FEATURE_COLS = [
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "liveness",
    "loudness",
    "speechiness",
    "tempo",
    "valence"
]

# weather/context features used to cluster recent listening events
WEATHER_FEATURE_COLS = [
    "temperature_c",
    "relative_humidity",
    "wind_speed_m_s"
]

# optional weather/context features if present
OPTIONAL_WEATHER_FEATURE_COLS = [
    "dewpoint_c",
    "visibility_m"
]


def _get_existing_weather_feature_cols(df):
    cols = [col for col in WEATHER_FEATURE_COLS if col in df.columns]
    cols += [col for col in OPTIONAL_WEATHER_FEATURE_COLS if col in df.columns]
    return cols


def _get_weather_model_cols(weather_kmeans, profiles_df):
    if hasattr(weather_kmeans, "feature_names_in_"):
        return list(weather_kmeans.feature_names_in_)
    return _get_existing_weather_feature_cols(profiles_df)


def _build_weather_prediction_frame(today_weather_df, profiles_df, weather_cols):
    profile_means = profiles_df[weather_cols].apply(pd.to_numeric, errors="coerce").mean()
    current_weather_df = today_weather_df.reindex(columns=weather_cols).copy()

    for col in weather_cols:
        current_weather_df[col] = pd.to_numeric(current_weather_df[col], errors="coerce")
        current_weather_df[col] = current_weather_df[col].fillna(profile_means[col])

    if current_weather_df[weather_cols].isna().any(axis=None):
        missing_cols = current_weather_df.columns[current_weather_df.isna().any()].tolist()
        raise ValueError(
            f"Today's weather input is missing usable values for required columns: {missing_cols}"
        )

    return current_weather_df[weather_cols]


def _get_audio_column_map(df):
    col_map = {}

    for col in AUDIO_FEATURE_COLS:
        if col in df.columns:
            col_map[col] = col
        elif f"recommended_{col}" in df.columns:
            col_map[col] = f"recommended_{col}"
        else:
            raise ValueError(
                f"Missing required audio feature column: '{col}' "
                f"(also checked for 'recommended_{col}')"
            )

    return col_map


def _validate_audio_cols(df):
    _get_audio_column_map(df)


# STEP 1: CLUSTER RECENTS BY WEATHER / CONTEXT
def cluster_recents_by_weather(
    input_file=None,
    output_file=None,
    profiles_output_file=None,
    df=None,
    n_clusters=4
):

    # Cluster recent listening events by weather/context only.
    
    """
    Input:
    - enriched recents file/dataframe
    - must already contain weather/context columns
    - must also contain audio feature columns

    Output:
    - clustered recent listening events
    - average audio profile for each learned weather cluster

    Important:
    - songs are NOT clustered by weather directly
    - listening events are clustered by weather/context
    - each weather cluster gets an average audio profile based on songs listened to in that context
    """
    
    # load data
    if df is None:
        if input_file is None:
            raise ValueError("input_file must be provided when df is None")
        df = pd.read_csv(input_file)

    df = df.copy()
    _validate_audio_cols(df)

    # ensure weather/context features are numeric and handle missing values
    weather_cols = _get_existing_weather_feature_cols(df)
    if not weather_cols:
        raise ValueError("No weather feature columns available for weather clustering")

    for col in weather_cols + AUDIO_FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    cluster_df = df.dropna(subset=weather_cols + AUDIO_FEATURE_COLS).copy()

    if cluster_df.empty:
        raise ValueError("No rows remain after dropping missing weather/audio values")

    # cluster by weather/context features only
    weather_X = cluster_df[weather_cols]
    distinct_weather_points = len(weather_X.drop_duplicates())
    n_clusters = min(n_clusters, len(cluster_df), distinct_weather_points)
    n_clusters = max(1, n_clusters)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_df["weather_cluster"] = kmeans.fit_predict(weather_X)

    # average song sound for each learned weather group
    profile_df = (
        cluster_df.groupby("weather_cluster")[AUDIO_FEATURE_COLS]
        .mean()
        .reset_index()
    )

    # keep summary of weather side of each cluster for interpretation/debugging
    weather_summary_df = (
        cluster_df.groupby("weather_cluster")[weather_cols]
        .mean()
        .reset_index()
    )

    # add dominant weather label for each cluster if available (e.g. "Rain", "Clear", etc.)
    if "weather_label" in cluster_df.columns:
        mode_weather = (
            cluster_df.groupby("weather_cluster")["weather_label"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA)
            .reset_index()
            .rename(columns={"weather_label": "dominant_weather_label"})
        )
        profile_df = profile_df.merge(mode_weather, on="weather_cluster", how="left")
    profile_df = profile_df.merge(weather_summary_df, on="weather_cluster", how="left")

    # save outputs to csv
    if output_file is not None:
        cluster_df.to_csv(output_file, index=False)

    if profiles_output_file is not None:
        profile_df.to_csv(profiles_output_file, index=False)

    return cluster_df, profile_df, kmeans


# STEP 2: ASSIGN NEW SONGS TO WEATHER-DERIVED GROUPS BY AUDIO SIMILARITY
def assign_songs_to_weather_groups_by_audio_similarity(
    songs_input=None,
    profiles_input=None,
    output_file=None,
    songs_df=None,
    profiles_df=None
):
    """
    New songs do NOT need weather data.

    These are assigned to a weather group by comparing their audio features
    to the average audio profile of each weather cluster.
    """
    if songs_df is None:
        if songs_input is None:
            raise ValueError("songs_input must be provided when songs_df is None")
        songs_df = pd.read_csv(songs_input)

    if profiles_df is None:
        if profiles_input is None:
            raise ValueError("profiles_input must be provided when profiles_df is None")
        profiles_df = pd.read_csv(profiles_input)

    songs_df = songs_df.copy()
    profiles_df = profiles_df.copy()

    songs_col_map = _get_audio_column_map(songs_df)
    profiles_col_map = _get_audio_column_map(profiles_df)

    for col in AUDIO_FEATURE_COLS:
        songs_df[songs_col_map[col]] = pd.to_numeric(songs_df[songs_col_map[col]], errors="coerce")
        profiles_df[profiles_col_map[col]] = pd.to_numeric(profiles_df[profiles_col_map[col]], errors="coerce")

    songs_clean = songs_df.dropna(subset=[songs_col_map[col] for col in AUDIO_FEATURE_COLS]).copy()
    profiles_clean = profiles_df.dropna(subset=[profiles_col_map[col] for col in AUDIO_FEATURE_COLS]).copy()

    if songs_clean.empty:
        raise ValueError("No songs remain after dropping missing audio feature values")

    if profiles_clean.empty:
        raise ValueError("No weather profiles remain after dropping missing audio feature values")

    songs_audio_df = pd.DataFrame({
        col: songs_clean[songs_col_map[col]]
        for col in AUDIO_FEATURE_COLS
    })

    profiles_audio_df = pd.DataFrame({
        col: profiles_clean[profiles_col_map[col]]
        for col in AUDIO_FEATURE_COLS
    })

    similarity_matrix = cosine_similarity(
        songs_audio_df,
        profiles_audio_df
    )

    best_profile_idx = similarity_matrix.argmax(axis=1)
    best_profile_scores = similarity_matrix.max(axis=1)

    songs_clean["weather_cluster"] = [
        profiles_clean.iloc[idx]["weather_cluster"] for idx in best_profile_idx
    ]
    songs_clean["weather_cluster_similarity"] = best_profile_scores

    if "dominant_weather_label" in profiles_clean.columns:
        songs_clean["assigned_weather_label"] = [
            profiles_clean.iloc[idx]["dominant_weather_label"] for idx in best_profile_idx
        ]

    if output_file is not None:
        songs_clean.to_csv(output_file, index=False)

    return songs_clean


# STEP 3: MAP TODAY'S WEATHER TO A LEARNED WEATHER CLUSTER
def get_weather_cluster_from_today_weather(
    today_weather_input=None,
    today_weather_df=None,
    profiles_input=None,
    profiles_df=None,
    weather_kmeans=None
):
    """
    Use today's weather/context row (already collected elsewhere) and map it
    to one of the learned weather clusters.

    today_weather_input should contain a row with weather fields like:
    temperature_c, relative_humidity, wind_speed_m_s, etc.
    """
    if today_weather_df is None:
        if today_weather_input is None:
            raise ValueError("today_weather_input must be provided when today_weather_df is None")
        today_weather_df = pd.read_csv(today_weather_input)

    if profiles_df is None:
        if profiles_input is None:
            raise ValueError("profiles_input must be provided when profiles_df is None")
        profiles_df = pd.read_csv(profiles_input)

    if weather_kmeans is None:
        raise ValueError("weather_kmeans is required")

    today_weather_df = today_weather_df.copy()
    profiles_df = profiles_df.copy()

    if len(today_weather_df) == 0:
        raise ValueError("today_weather_df is empty")

    weather_cols = _get_weather_model_cols(weather_kmeans, profiles_df)
    if not weather_cols:
        raise ValueError("No weather feature columns found in profiles")

    current_weather_df = _build_weather_prediction_frame(
        today_weather_df=today_weather_df,
        profiles_df=profiles_df,
        weather_cols=weather_cols
    )
    cluster_id = int(weather_kmeans.predict(current_weather_df[weather_cols])[0])

    matching_profile = profiles_df[profiles_df["weather_cluster"] == cluster_id].copy()

    return cluster_id, matching_profile


# STEP 4: FILTER SONGS FOR TODAY'S WEATHER GROUP
def filter_songs_for_today_weather_cluster(
    assigned_songs_input=None,
    assigned_songs_df=None,
    target_cluster=None,
    output_file=None
):
    """
    Keep only songs that belong to the weather-derived music group
    for TODAY's weather cluster.
    """
    if assigned_songs_df is None:
        if assigned_songs_input is None:
            raise ValueError("assigned_songs_input must be provided when assigned_songs_df is None")
        assigned_songs_df = pd.read_csv(assigned_songs_input)

    if target_cluster is None:
        raise ValueError("target_cluster is required")

    assigned_songs_df = assigned_songs_df.copy()
    filtered_df = assigned_songs_df[assigned_songs_df["weather_cluster"] == target_cluster].copy()

    if output_file is not None:
        filtered_df.to_csv(output_file, index=False)

    return filtered_df
    

if __name__ == "__main__":
    # 1) cluster enriched recents by weather/context
    clustered_recents_df, weather_audio_profiles_df, weather_kmeans = cluster_recents_by_weather(
        input_file="data/recent_tracks_enriched_weather.csv",
        output_file="data/recent_tracks_enriched_weather_clustered.csv",
        profiles_output_file="data/weather_audio_profiles.csv",
        n_clusters=4
    )

    # 2) assign candidate songs/catalog songs into those weather-derived groups by audio similarity
    assigned_songs_df = assign_songs_to_weather_groups_by_audio_similarity(
        songs_input="data/track_recommendations.csv",
        profiles_input="data/weather_audio_profiles.csv",
        output_file="data/track_recommendations_weather_assigned.csv"
    )

    # 3) map today's weather row (collected elsewhere) into one of the learned weather clusters
    today_cluster_id, today_profile_df = get_weather_cluster_from_today_weather(
        today_weather_input="data/today_weather.csv",
        profiles_input="data/weather_audio_profiles.csv",
        weather_kmeans=weather_kmeans
    )

    # 4) keep only songs that match today's weather-derived music group
    today_weather_playlist_df = filter_songs_for_today_weather_cluster(
        assigned_songs_input="data/track_recommendations_weather_assigned.csv",
        target_cluster=today_cluster_id,
        output_file="data/today_weather_playlist_candidates.csv"
    )
    
    print(clustered_recents_df["weather_cluster"].value_counts().sort_index())
    
