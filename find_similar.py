import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


def get_top_k_similar_songs(query_input, catalog_features_input, catalog_lookup_input, k=10):
    # allow file path or dataframe for query
    if isinstance(query_input, str):
        query_df = pd.read_csv(query_input)
    else:
        query_df = query_input.copy()

    # allow file path or dataframe for catalog features
    if isinstance(catalog_features_input, str):
        catalog_features_df = pd.read_csv(catalog_features_input)
    else:
        catalog_features_df = catalog_features_input.copy()

    # allow file path or dataframe for lookup
    if isinstance(catalog_lookup_input, str):
        catalog_lookup_df = pd.read_csv(catalog_lookup_input)
    else:
        catalog_lookup_df = catalog_lookup_input.copy()

    # remove track_id before similarity math
    query_feature_df = query_df.drop(columns=["track_id"], errors="ignore")
    catalog_feature_df = catalog_features_df.drop(columns=["track_id"], errors="ignore")

    # cosine similarity: each query song against whole catalog
    sim_matrix = cosine_similarity(query_feature_df, catalog_feature_df)

    all_results = []
    for query_idx in range(len(query_df)):
        sim_scores = sim_matrix[query_idx]

        sorted_indices = sim_scores.argsort()[::-1]

        top_indices = []
        for idx in sorted_indices:
            # avoid recommending the same track
            if "track_id" in query_df.columns and "track_id" in catalog_lookup_df.columns:
                if query_df.iloc[query_idx]["track_id"] == catalog_lookup_df.iloc[idx]["track_id"]:
                    continue

            top_indices.append(idx)
            if len(top_indices) == k:
                break

        query_track_id = query_df.iloc[query_idx]["track_id"]
        
        for rank, catalog_idx in enumerate(top_indices, start=1):
            row = {
                "query_track_id": query_track_id,
                "similarity_score": sim_scores[catalog_idx],
                "rank": rank
            }

            # add ALL lookup/catalog metadata
            lookup_row = catalog_lookup_df.iloc[catalog_idx].to_dict()
            for col, val in lookup_row.items():
                row[f"recommended_{col}"] = val

            # add ALL feature columns too
            feature_row = catalog_features_df.iloc[catalog_idx].to_dict()
            for col, val in feature_row.items():
                if col != "track_id":
                    row[f"recommended_{col}"] = val

            all_results.append(row)

    return pd.DataFrame(all_results)


if __name__ == "__main__":
    results_df = get_top_k_similar_songs(
        query_input="data/top_scaled_features.csv",
        catalog_features_input="data/all_scaled_features.csv",
        catalog_lookup_input="data/all_tracks_lookup.csv",
        k=5
    )

    results_df.to_csv("data/track_recommendations.csv", index=False)