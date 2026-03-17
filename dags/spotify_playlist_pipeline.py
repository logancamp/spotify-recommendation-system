from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Repo is the source of truth
REPO_ROOT = "/D/tdo18/OneDrive/CSDS417/spotify-recommendation-system"
PIPELINE_DIR = f"{REPO_ROOT}/pipeline/spotify_pipeline"
ANALYTICS_DIR = REPO_ROOT

with DAG(
    dag_id="spotify_playlist_pipeline",
    start_date=datetime(2024, 2, 1),
    schedule=None,
    catchup=False,
    tags=["csds417", "spotify", "reccobeats", "data-engineering"],
) as dag:

    extract_and_store_top_tracks = BashOperator(
        task_id="extract_and_store_top_tracks",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/ingest_spotify_top_tracks.py",
    )

    enrich_audio_features = BashOperator(
        task_id="enrich_audio_features",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/enrich_audio_features_from_reccobeats.py",
    )

    upload_missing_report = BashOperator(
        task_id="upload_missing_audio_features_report",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/upload_missing_audio_features_report.py",
    )

    run_audio_clustering = BashOperator(
        task_id="run_audio_clustering",
        bash_command=f"cd {ANALYTICS_DIR} && python cluster.py",
    )

    build_candidate_pool = BashOperator(
        task_id="build_candidate_pool",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/build_catalog_spotify_search.py",
    )

    enrich_candidate_audio_features = BashOperator(
        task_id="enrich_candidate_audio_features",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/enrich_catalog_audio_features.py",
    )

    upload_candidate_search_raw_to_s3 = BashOperator(
        task_id="upload_candidate_search_raw_to_s3",
        bash_command=f"cd {PIPELINE_DIR} && python scripts/upload_candidate_search_raw_to_s3.py",
    )

    upload_ranked_recommendations_to_s3 = BashOperator(
       task_id="upload_ranked_recommendations_to_s3",
       bash_command=f"cd {ANALYTICS_DIR} && python upload_ranked_csv_to_s3.py",
    )

    extract_and_store_top_tracks >> enrich_audio_features >> upload_missing_report >> build_candidate_pool >> upload_candidate_search_raw_to_s3 >> enrich_candidate_audio_features >> run_audio_clustering >> upload_ranked_recommendations_to_s3
