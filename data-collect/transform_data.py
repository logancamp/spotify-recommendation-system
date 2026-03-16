import pandas as pd

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

METADATA_COLS = [
    "isrc",
    "duration_ms",
    "explicit",
    "added_at",
    "features_found",
    "match_source"
]

KEEP_COLS = LOOKUP_COLS + METADATA_COLS + FEATURE_COLS

# basic cleaning for all files - remove nulls, strip whitespace, remove duplicates
def clean_file(input, output, df=None, required_columns=None, keep_columns=None):
    if df is None:
        df = pd.read_csv(input)

    # strip whitespace
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # remove missing data
    df = df.replace(["", "NA", "N/A", "null", "None", "<NA>"], pd.NA)
    df = df.dropna(axis=1, how="all")
    if required_columns:
        required_subset = [col for col in required_columns if col in df.columns]
        if required_subset:
            df = df.dropna(subset=required_subset)
    else:
        df = df.dropna(axis=0, how="all")

    if keep_columns:
        selected_columns = [col for col in keep_columns if col in df.columns]
        if selected_columns:
            df = df[selected_columns]

    # remove duplicates and reset index
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    df.to_csv(output, index=False)
    return df, output


# merge data for all files (robust to duplicate entries) on track_id
def merge_on_track_id(base_file, extra_file, output):
    base_df = pd.read_csv(base_file)
    extra_df = pd.read_csv(extra_file)

    # remove duplicate track_id rows first
    base_df = base_df.drop_duplicates(subset="track_id")
    extra_df = extra_df.drop_duplicates(subset="track_id")

    # only bring over columns that do not already exist in base_df
    extra_cols_to_add = ["track_id"] + [
        col for col in extra_df.columns
        if col != "track_id" and col not in base_df.columns
    ]

    merged_df = base_df.merge(
        extra_df[extra_cols_to_add],
        on="track_id",
        how="left"
    )

    merged_df, output = clean_file(None, output, merged_df)
    return merged_df, output


if __name__ == "__main__":
    TRACKS_FILE = "data/saved_tracks_enriched.csv"
    OUTPUT_TRACKS_FILE = "data/cleaned_tracks.csv"
    clean_file(
        TRACKS_FILE,
        OUTPUT_TRACKS_FILE,
        required_columns=LOOKUP_COLS + FEATURE_COLS,
        keep_columns=KEEP_COLS
    )

    RECENTS_FILE = "data/recent_tracks_enriched.csv"
    OUTPUT_RECENTS_FILE = "data/cleaned_recents.csv"
    clean_file(
        RECENTS_FILE,
        OUTPUT_RECENTS_FILE,
        required_columns=LOOKUP_COLS + FEATURE_COLS,
        keep_columns=KEEP_COLS
    )

    TOP_FILE = "data/top_tracks_enriched.csv"
    OUTPUT_TOP_FILE = "data/cleaned_top.csv"
    clean_file(
        TOP_FILE,
        OUTPUT_TOP_FILE,
        required_columns=LOOKUP_COLS + FEATURE_COLS,
        keep_columns=KEEP_COLS
    )

    # Left join if needed. 
    # MERGE_RECENTS_FILE = ""
    # MERGE_TOP_FILE = ""
    # merge_on_track_id(OUTPUT_RECENTS_FILE, OUTPUT_TRACKS_FILE, MERGE_RECENTS_FILE)
    # merge_on_track_id(OUTPUT_TOP_FILE, OUTPUT_TRACKS_FILE, MERGE_TOP_FILE)
