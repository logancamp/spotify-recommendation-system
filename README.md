# Spotify Recommendation System (CSDS 417)
**Pipeline:** Airflow (VM) + Spotify API + ReccoBeats + S3 (raw) + Postgres (normalized)  
**Analytics (Strategy B):** Clustering reads directly from Postgres (no CSV required)

## Repo Structure
- `pipeline/spotify_pipeline/` — ingestion + enrichment scripts + SQL schema
- `dags/spotify_playlist_pipeline.py` — Airflow DAG (source-controlled)
- `cluster.py` — audio-only clustering using Postgres (Strategy B)
- `db_utils.py` — shared Postgres connector for analytics scripts
- `data/` — generated outputs (clustered CSVs for inspection)

## Setup (VM)
### 1) Install Python dependencies
From repo root:
```bash
pip install -r requirements.txt
