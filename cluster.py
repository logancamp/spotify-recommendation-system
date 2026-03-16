import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

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
    """
    Cluster recent listening events by weather/context only.

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
    if df is None:
        if input_file is None:
            raise ValueError("input_file must be provided when df is None")
        df = pd.read_csv(input_file)

    df = df.copy()
    _validate_audio_cols(df)

    weather_cols = _get_existing_weather_feature_cols(df)
    if not weather_cols:
        raise ValueError("No weather feature columns available for weather clustering")

    for col in weather_cols + AUDIO_FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    cluster_df = df.dropna(subset=weather_cols + AUDIO_FEATURE_COLS).copy()

    if cluster_df.empty:
        raise ValueError("No rows remain after dropping missing weather/audio values")

    if len(cluster_df) < n_clusters:
        n_clusters = max(1, len(cluster_df))

    weather_scaler = StandardScaler()
    weather_X = weather_scaler.fit_transform(cluster_df[weather_cols])

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

    if "weather_label" in cluster_df.columns:
        mode_weather = (
            cluster_df.groupby("weather_cluster")["weather_label"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA)
            .reset_index()
            .rename(columns={"weather_label": "dominant_weather_label"})
        )
        profile_df = profile_df.merge(mode_weather, on="weather_cluster", how="left")

    profile_df = profile_df.merge(weather_summary_df, on="weather_cluster", how="left")

    if output_file is not None:
        cluster_df.to_csv(output_file, index=False)

    if profiles_output_file is not None:
        profile_df.to_csv(profiles_output_file, index=False)

    return cluster_df, profile_df, weather_scaler, kmeans


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

    They are assigned to a weather-derived group by comparing their audio features
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
    weather_scaler=None,
    weather_kmeans=None
):
    """
    Use today's weather/context row (already collected elsewhere) and map it
    to one of the learned weather clusters.

    today_weather_input should contain one row with weather/context fields like:
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

    if weather_scaler is None or weather_kmeans is None:
        raise ValueError("weather_scaler and weather_kmeans are required")

    today_weather_df = today_weather_df.copy()
    profiles_df = profiles_df.copy()

    if len(today_weather_df) == 0:
        raise ValueError("today_weather_df is empty")

    weather_cols = _get_existing_weather_feature_cols(profiles_df)
    if not weather_cols:
        raise ValueError("No weather feature columns found in profiles")

    for col in weather_cols:
        if col in today_weather_df.columns:
            today_weather_df[col] = pd.to_numeric(today_weather_df[col], errors="coerce")

    current_weather_df = today_weather_df[weather_cols].copy()
    current_weather_df = current_weather_df.dropna(axis=1, how="all")

    weather_cols = [col for col in weather_cols if col in current_weather_df.columns]
    if not weather_cols:
        raise ValueError("No usable weather columns available in today's weather input")

    current_X = weather_scaler.transform(current_weather_df[weather_cols])
    cluster_id = int(weather_kmeans.predict(current_X)[0])

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
    matching today's weather cluster.
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

def plot_weather_clusters(
    clustered_df,
    weather_cols,
    weather_scaler,
    weather_kmeans,
    output_file=None
):
    """
    Visualize weather/context clusters in 2D using PCA.

    - clustered_df must already contain weather_cluster
    - weather_cols are the weather/context feature columns used for clustering
    - weather_scaler and weather_kmeans must be the fitted objects from training
    """
    plot_df = clustered_df.copy()

    for col in weather_cols:
        plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")

    plot_df = plot_df.dropna(subset=weather_cols + ["weather_cluster"]).copy()

    if plot_df.empty:
        raise ValueError("No rows available to plot after dropping missing values")

    # scale the same weather features used during clustering
    X_scaled = weather_scaler.transform(plot_df[weather_cols])

    # reduce weather feature space to 2D for visualization
    pca = PCA(n_components=2)
    X_2d = pca.fit_transform(X_scaled)

    plot_df["pca_1"] = X_2d[:, 0]
    plot_df["pca_2"] = X_2d[:, 1]

    # project cluster centers into the same 2D PCA space
    centers_2d = pca.transform(weather_kmeans.cluster_centers_)

    plt.figure(figsize=(10, 7))

    # plot each cluster separately for cleaner legend handling
    for cluster_id in sorted(plot_df["weather_cluster"].unique()):
        cluster_points = plot_df[plot_df["weather_cluster"] == cluster_id]

        plt.scatter(
            cluster_points["pca_1"],
            cluster_points["pca_2"],
            label=f"Cluster {cluster_id}",
            alpha=0.7
        )

    # plot centroids
    plt.scatter(
        centers_2d[:, 0],
        centers_2d[:, 1],
        marker="X",
        s=200,
        linewidths=1.5,
        label="Centroids"
    )

    plt.title("Weather Cluster Visualization (PCA)")
    plt.xlabel("PCA Component 1")
    plt.ylabel("PCA Component 2")
    plt.legend()
    plt.tight_layout()

    if output_file is not None:
        plt.savefig(output_file, dpi=300, bbox_inches="tight")

    plt.show()
    

# RUN
if __name__ == "__main__":
    # 1) cluster enriched recents by weather/context
    clustered_recents_df, weather_audio_profiles_df, weather_scaler, weather_kmeans = cluster_recents_by_weather(
        input_file="data/recently_played_weather_enriched.csv",
        output_file="data/recently_played_weather_clustered.csv",
        profiles_output_file="data/weather_audio_profiles.csv",
        n_clusters=4
    )

    # 2) assign candidate songs/catalog songs into those weather-derived groups by audio similarity
    assigned_songs_df = assign_songs_to_weather_groups_by_audio_similarity(
        songs_input="data/top_track_recommendations.csv",
        profiles_input="data/weather_audio_profiles.csv",
        output_file="data/top_track_recommendations_weather_assigned.csv"
    )

    # 3) map today's weather row (collected elsewhere) into one of the learned weather clusters
    today_cluster_id, today_profile_df = get_weather_cluster_from_today_weather(
        today_weather_input="data/today_weather.csv",
        profiles_input="data/weather_audio_profiles.csv",
        weather_scaler=weather_scaler,
        weather_kmeans=weather_kmeans
    )

    # 4) keep only songs that match today's weather-derived music group
    today_weather_playlist_df = filter_songs_for_today_weather_cluster(
        assigned_songs_input="data/top_track_recommendations_weather_assigned.csv",
        target_cluster=today_cluster_id,
        output_file="data/today_weather_playlist_candidates.csv"
    )

    # 5) visualize the weather clusters
    weather_cols = _get_existing_weather_feature_cols(clustered_recents_df)
    plot_weather_clusters(
        clustered_df=clustered_recents_df,
        weather_cols=weather_cols,
        weather_scaler=weather_scaler,
        weather_kmeans=weather_kmeans,
        output_file="data/weather_clusters_plot.png"
    )
    print(clustered_recents_df["weather_cluster"].value_counts().sort_index())
    