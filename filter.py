import pandas as pd

ATTRIBUTE_RULES = {
    "high_tempo": {
        "column": "recommended_tempo",
        "yes": lambda s: s >= 120,
        "no": lambda s: s < 120,
        "score_yes": lambda s: (s / 120.0).clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - (s / 120.0 - 1.0).clip(lower=0.0, upper=1.0)).clip(lower=0.0, upper=1.0),
    },
    "happy": {
        "column": "recommended_valence",
        "yes": lambda s: s >= 0.60,
        "no": lambda s: s < 0.60,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "sad": {
        "column": "recommended_valence",
        "yes": lambda s: s <= 0.40,
        "no": lambda s: s > 0.40,
        "score_yes": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
        "score_no": lambda s: s.clip(lower=0.0, upper=1.0),
    },
    "high_energy": {
        "column": "recommended_energy",
        "yes": lambda s: s >= 0.70,
        "no": lambda s: s < 0.70,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "danceable": {
        "column": "recommended_danceability",
        "yes": lambda s: s >= 0.65,
        "no": lambda s: s < 0.65,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "acoustic": {
        "column": "recommended_acousticness",
        "yes": lambda s: s >= 0.50,
        "no": lambda s: s < 0.50,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "instrumental": {
        "column": "recommended_instrumentalness",
        "yes": lambda s: s >= 0.50,
        "no": lambda s: s < 0.50,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "live_sounding": {
        "column": "recommended_liveness",
        "yes": lambda s: s >= 0.30,
        "no": lambda s: s < 0.30,
        "score_yes": lambda s: s.clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - s).clip(lower=0.0, upper=1.0),
    },
    "speech_heavy": {
        "column": "recommended_speechiness",
        "yes": lambda s: s >= 0.10,
        "no": lambda s: s < 0.10,
        "score_yes": lambda s: (s / 0.10).clip(lower=0.0, upper=1.0),
        "score_no": lambda s: (1.0 - (s / 0.10).clip(lower=0.0, upper=1.0)).clip(lower=0.0, upper=1.0),
    },
}


def validate_required_columns(data_df: pd.DataFrame, survey_df: pd.DataFrame) -> None:
    required_columns = []

    for _, row in survey_df.iterrows():
        attribute = row["attribute"]
        choice = row["choice"]

        if pd.isna(choice):
            continue

        if attribute not in ATTRIBUTE_RULES:
            continue

        column = ATTRIBUTE_RULES[attribute]["column"]
        if column not in required_columns:
            required_columns.append(column)

    missing = []

    for column in required_columns:
        if column not in data_df.columns:
            missing.append(column)

    if missing:
        raise ValueError(f"Missing required columns in input file: {missing}")


def apply_attribute_filters(data_df: pd.DataFrame, survey_df: pd.DataFrame) -> pd.DataFrame:
    filtered_df = data_df.copy()

    for _, row in survey_df.iterrows():
        attribute = row["attribute"]
        choice = row["choice"]

        if pd.isna(choice):
            continue

        if attribute not in ATTRIBUTE_RULES:
            continue

        choice = str(choice).strip().lower()
        if choice not in ("yes", "no"):
            continue

        rule = ATTRIBUTE_RULES[attribute]
        column = rule["column"]

        mask = rule[choice](filtered_df[column])
        filtered_df = filtered_df[mask]

    return filtered_df.reset_index(drop=True)


def compute_preference_match_scores(data_df: pd.DataFrame, survey_df: pd.DataFrame) -> pd.DataFrame:
    scored_df = data_df.copy()
    active_attributes = []

    for _, row in survey_df.iterrows():
        attribute = row["attribute"]
        choice = row["choice"]

        if pd.isna(choice):
            continue

        if attribute not in ATTRIBUTE_RULES:
            continue

        choice = str(choice).strip().lower()
        if choice not in ("yes", "no"):
            continue

        active_attributes.append((attribute, choice))

    if not active_attributes:
        scored_df["survey_match_score"] = 1.0
        return scored_df

    total_score = pd.Series(0.0, index=scored_df.index)

    for attribute, choice in active_attributes:
        rule = ATTRIBUTE_RULES[attribute]
        column = rule["column"]

        if choice == "yes":
            feature_score = rule["score_yes"](scored_df[column].astype(float))
        else:
            feature_score = rule["score_no"](scored_df[column].astype(float))

        total_score = total_score + feature_score

    scored_df["survey_match_score"] = total_score / float(len(active_attributes))
    return scored_df.sort_values("survey_match_score", ascending=False).reset_index(drop=True)


def filter_file(
    input_csv_path: str,
    survey_df: pd.DataFrame,
    output_csv_path: str,
    min_match_score: float = 0.0,
    min_results: int = 5
) -> pd.DataFrame:
    data_df = pd.read_csv(input_csv_path)

    validate_required_columns(data_df, survey_df)

    filtered_df = apply_attribute_filters(data_df, survey_df)

    if len(filtered_df) > 0:
        result_df = compute_preference_match_scores(filtered_df, survey_df)
    else:
        result_df = compute_preference_match_scores(data_df, survey_df)

    if min_match_score > 0.0:
        filtered_by_score = result_df[result_df["survey_match_score"] >= min_match_score]

        if len(filtered_by_score) >= min_results:
            result_df = filtered_by_score.reset_index(drop=True)
        else:
            result_df = result_df.head(min_results).reset_index(drop=True)
    
    sort_columns = ["survey_match_score"]
    ascending = [False]

    if "similarity_score" in result_df.columns:
        sort_columns.append("similarity_score")
        ascending.append(False)

    if "rank" in result_df.columns:
        sort_columns.append("rank")
        ascending.append(True)

    result_df = result_df.sort_values(
        by=sort_columns,
        ascending=ascending
    ).reset_index(drop=True)

    result_df.to_csv(output_csv_path, index=False)
    return result_df


if __name__ == "__main__":
    input_file = "data/track_recommendations.csv"
    output_file = "data/track_recommendations_filtered.csv"

    survey_data = {
        "attribute": [
            "high_tempo",
            "happy",
            "sad",
            "high_energy",
            "danceable",
            "acoustic",
            "instrumental",
            "live_sounding",
            "speech_heavy",
        ],
        "choice": [
            "no",
            "no",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    }

    survey_df = pd.DataFrame(survey_data)

    result_df = filter_file(
        input_csv_path=input_file,
        survey_df=survey_df,
        output_csv_path=output_file,
        min_match_score=0.0,
        min_results = 5
    )

    print("Done.")
    print(f"Rows returned: {len(result_df)}")

    preview_columns = [
        col for col in [
            "recommended_track_name",
            "recommended_artist_names",
            "survey_match_score",
            "similarity_score",
            "rank",
            "recommended_tempo",
            "recommended_valence",
            "recommended_energy",
            "recommended_danceability",
            "recommended_acousticness",
        ]
        if col in result_df.columns
    ]

    print(result_df[preview_columns].head(10).to_string(index=False))