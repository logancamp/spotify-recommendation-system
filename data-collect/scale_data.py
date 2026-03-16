import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler

FEATURE_COLS = [
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "key",
    "liveness",
    "loudness",
    "mode",
    "speechiness",
    "tempo",
    "valence"
]

LOOKUP_COLS = [
    "track_id",
    "track_name",
    "artist_names"
]


# save lookup and scaled features for tracks
def save_track_lookup(df, output_file):
    lookup_df = df[LOOKUP_COLS].copy()
    lookup_df.to_csv(output_file, index=False)
    return lookup_df


def save_scaled_features(df, features_output, scaler_output):
    feature_df = df[FEATURE_COLS].copy()

    # scale the features using StandardScaler
    scaler = StandardScaler()
    scaled_array = scaler.fit_transform(feature_df)

    scaled_df = pd.DataFrame(
        scaled_array,
        columns=FEATURE_COLS,
        index=df.index
    )

    # save the scaled features and the scaler object
    scaled_output_df = df[["track_id"]].copy()
    scaled_output_df[FEATURE_COLS] = scaled_df
    scaled_output_df.to_csv(features_output, index=False)
    joblib.dump(scaler, scaler_output)

    return scaled_df, scaler


if __name__ == "__main__":
    TOP_TRACKS_FILE = "data/cleaned_top.csv"
    TOP_OUTPUT_FILES = {"lookup": "data/top_tracks_lookup.csv",
                        "features": "data/top_scaled_features.csv",
                        "scaler": "data/top_feature_scaler.pkl"}

    df = pd.read_csv(TOP_TRACKS_FILE)
    # human readable data for lookup and analysis
    lookup_df = save_track_lookup(df, TOP_OUTPUT_FILES["lookup"])

    # save scaled features for clustering and similarity calculations
    # and save scaler for later use on new data
    scaled_df, scaler = save_scaled_features(df, 
                                             TOP_OUTPUT_FILES["features"], 
                                             TOP_OUTPUT_FILES["scaler"])
    
    
    RECENT_TRACKS_FILE = "data/cleaned_recents.csv"
    RECENT_OUTPUT_FILES = {"lookup": "data/recent_tracks_lookup.csv",
                            "features": "data/recent_scaled_features.csv",
                            "scaler": "data/recent_feature_scaler.pkl"}

    df = pd.read_csv(RECENT_TRACKS_FILE)
    # human readable data for lookup and analysis
    lookup_df = save_track_lookup(df, RECENT_OUTPUT_FILES["lookup"])

    # save scaled features for clustering and similarity calculations
    # and save scaler for later use on new data
    scaled_df, scaler = save_scaled_features(df, 
                                             RECENT_OUTPUT_FILES["features"], 
                                             RECENT_OUTPUT_FILES["scaler"])
    
    ALL_TRACKS_FILE = "data/cleaned_tracks.csv"
    ALL_OUTPUT_FILES = {"lookup": "data/all_tracks_lookup.csv",
                            "features": "data/all_scaled_features.csv",
                            "scaler": "data/all_feature_scaler.pkl"}

    df = pd.read_csv(ALL_TRACKS_FILE)
    # human readable data for lookup and analysis
    lookup_df = save_track_lookup(df, ALL_OUTPUT_FILES["lookup"])

    # save scaled features for clustering and similarity calculations
    # and save scaler for later use on new data
    scaled_df, scaler = save_scaled_features(df, 
                                             ALL_OUTPUT_FILES["features"], 
                                             ALL_OUTPUT_FILES["scaler"])