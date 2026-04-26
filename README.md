# Spotify Recommendation System (CSDS 417)

A fully automated, end-to-end music recommendation pipeline that ingests your Spotify listening history, clusters your taste using KMeans, finds similar candidate tracks, optionally reranks them using real-time weather data, and lets you generate and save a personalized playlist through a Streamlit web app.

---

## How It Works

1. **Ingest** — pulls your top 50 tracks (short/medium/long term) from the Spotify API into PostgreSQL
2. **Enrich** — fetches audio features (energy, valence, danceability, etc.) for every track via the ReccoBeats API
3. **Build candidates** — searches Spotify for tracks from your top artists/genres that you haven't heard
4. **Cluster** — runs KMeans on your listening history to find your audio "taste clusters", then scores candidates by cosine similarity to those centroids
5. **Weather reranking** — optionally blends in a weather context score (rainy → acoustic, hot → danceable, etc.)
6. **Survey reranking** — in the web app, you pick mood preferences (Happy, Acoustic, High Energy, etc.) which rerank the results on the fly
7. **Save** — one click saves the final playlist directly to your Spotify account

The pipeline runs automatically at **8 AM and 8 PM daily** via Apache Airflow. New users can also trigger it on-demand by logging in through the web app.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11 |
| Database | PostgreSQL (`129.22.23.235:5455`, db: `spotify_recsys`) |
| Orchestration | Apache Airflow (cron: `0 8,20 * * *`) |
| Web UI | Streamlit (port 8501) |
| Music data | Spotify Web API |
| Audio features | ReccoBeats API |
| Weather data | NOAA National Weather Service API |
| Raw storage | AWS S3 |
| ML | scikit-learn (KMeans, cosine similarity) |
| DB access | SQLAlchemy + psycopg2 |

---

## Repo Structure

```
spotify-recommendation-system/
├── main.py                        # Streamlit web app (login → survey → playlist)
├── cluster.py                     # KMeans clustering + candidate ranking
├── pipeline_runner.py             # Per-user on-demand pipeline executor
├── db_utils.py                    # Shared PostgreSQL helpers
├── create_playlist_from_ranked.py # Standalone: create Spotify playlist from DB
├── upload_ranked_csv_to_s3.py     # Standalone: upload ranked CSV to S3
├── requirements.txt
├── .env.example                   # Template for credentials
├── dags/
│   └── spotify_playlist_pipeline.py  # Airflow DAG definition
├── pipeline/
│   └── spotify_pipeline/
│       ├── scripts/               # All pipeline scripts
│       └── sql/
│           └── schema.sql         # PostgreSQL DDL (run once to set up tables)
└── data/                          # Generated CSVs (gitignored)
```

---

## Setup

### Prerequisites
- Python 3.11 with conda
- Access to the shared PostgreSQL server
- Spotify Developer App credentials (client ID + secret)
- AWS Learner Lab credentials

### 1. Clone and set up the environment

```bash
git clone <repo-url>
cd spotify-recommendation-system

conda create -n airflow_env python=3.11
conda activate airflow_env
pip install -r requirements.txt
pip install pytz plotly
```

### 2. Configure credentials

```bash
cp .env.example .env
# Open .env and fill in all values
```

Key variables in `.env`:

| Variable | Purpose |
|---|---|
| `SPOTIPY_CLIENT_ID` | Spotify app client ID |
| `SPOTIPY_CLIENT_SECRET` | Spotify app client secret |
| `SPOTIPY_REDIRECT_URI` | OAuth redirect (e.g. `http://127.0.0.1:8501`) |
| `SPOTIFY_REFRESH_TOKEN` | Long-lived token for Airflow DAG |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | PostgreSQL connection |
| `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, `AWS_SESSION_TOKEN` | AWS Learner Lab credentials |
| `S3_BUCKET_NAME` | S3 bucket name |

### 3. Set up the database (first time only)

```bash
psql -h 129.22.23.235 -p 5455 -U postgres_admin -d spotify_recsys \
     -f pipeline/spotify_pipeline/sql/schema.sql
```

### 4. Run the Streamlit app

```bash
conda run -n airflow_env streamlit run main.py --server.port 8501
```

Open `http://127.0.0.1:8501` in your browser, log in with Spotify, and the pipeline will run automatically for your account.

> **Note:** Your Spotify account must be added to the app's User Management in the Spotify Developer Dashboard while the app is in Development Mode.

---

## Airflow (Scheduled Pipeline)

The Airflow DAG handles automatic runs twice a day using a server-side refresh token — no user interaction needed.

### Start Airflow

Open two terminals:

```bash
# Terminal 1
conda activate airflow_env && airflow scheduler

# Terminal 2
conda activate airflow_env && airflow webserver --port 8080
```

Then go to `http://localhost:8080`, find `spotify_playlist_pipeline`, and make sure it is **unpaused**.

### DAG symlink (required once)

```bash
ln -s /D/tdo18/Documents/spotify-recommendation-system/dags/spotify_playlist_pipeline.py \
      /D/tdo18/airflow/dags/spotify_playlist_pipeline.py
```

### Manually trigger a run

```bash
conda run -n airflow_env airflow dags trigger spotify_playlist_pipeline
```

---

## Web App Pages

| Page | What it does |
|---|---|
| **Login** | Spotify OAuth — authorizes the app to read your top tracks and create playlists |
| **Loading** | Checks if recommendations are < 12 hours old; if fresh skips the pipeline, otherwise runs all 5 steps with a live progress UI |
| **Survey** | Pick mood preferences (want/don't want), temperature slider, number of songs, playlist name, weather toggle |
| **Playlist** | Shows the reranked songs, a radar chart of audio features for any selected track, and a Save to Spotify button |

---

## Database Tables

| Table | Description |
|---|---|
| `users` | One row per Spotify account |
| `tracks` | Global catalog of all tracks seen by the system |
| `user_top_tracks` | Current top tracks per user (stale rows deleted each run) |
| `catalog_tracks` | Candidate tracks per user (built from artist/genre searches) |
| `audio_features` | ReccoBeats audio features for every track |
| `ranked_recommendations` | Final scored + ranked output of `cluster.py` per user |
| `context_inputs` | Recent weather observations from NOAA NWS |

---

## Notes

- AWS Learner Lab credentials expire every few hours — update `.env` when S3 tasks start failing
- Spotify access tokens expire after 1 hour; if Save to Spotify fails, log out and back in
- The app is in Spotify Development Mode (max 25 users) — add accounts in the Developer Dashboard
