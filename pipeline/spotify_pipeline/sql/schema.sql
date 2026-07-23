-- CSDS 417 Spotify Pipeline (Tola)
-- Primary join key across the system: spotify_track_id
-- ReccoBeats returns reccobeats_track_uuid + href + isrc via /v1/audio-features (Subject to change maybe)

CREATE TABLE IF NOT EXISTS users (
  user_id SERIAL PRIMARY KEY,
  spotify_user_hash TEXT UNIQUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tracks (
  spotify_track_id TEXT PRIMARY KEY,
  name TEXT,
  primary_artist TEXT,
  popularity INTEGER,
  duration_ms INTEGER,
  explicit BOOLEAN,
  spotify_url TEXT,
  isrc TEXT,
  raw_json JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_top_tracks (
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,
  rank INTEGER,
  time_range TEXT CHECK (time_range IN ('short_term','medium_term','long_term')),
  pulled_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, spotify_track_id, time_range)
);

CREATE TABLE IF NOT EXISTS catalog_tracks (
  catalog_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE SET NULL,

  source TEXT,
  query_used TEXT,
  seed_type TEXT,
  seed_value TEXT,
  pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE SET NULL,

  pulled_at TIMESTAMPTZ DEFAULT NOW(),
  raw_json JSONB,

  UNIQUE (user_id, spotify_track_id, seed_type, seed_value)
);

-- Audio features keyed by Spotify track id (since /v1/audio-features accepts Spotify IDs)
CREATE TABLE IF NOT EXISTS audio_features (
  spotify_track_id TEXT PRIMARY KEY REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,

  -- Bridge fields returned by ReccoBeats
  reccobeats_track_uuid TEXT,   -- UUID returned in response field 'id'
  href TEXT,                    -- Spotify URL returned by ReccoBeats
  isrc TEXT,

  -- Audio feature floats
  acousticness REAL,
  danceability REAL,
  energy REAL,
  instrumentalness REAL,
  key INTEGER,
  liveness REAL,
  loudness REAL,
  mode INTEGER,
  speechiness REAL,
  tempo REAL,
  valence REAL,

  source TEXT DEFAULT 'reccobeats',
  pulled_at TIMESTAMPTZ DEFAULT NOW(),
  raw_json JSONB
);

-- Indexes for pipeline queries
CREATE INDEX IF NOT EXISTS idx_user_top_tracks_user_time
  ON user_top_tracks(user_id, time_range, pulled_at);

CREATE INDEX IF NOT EXISTS idx_catalog_tracks_spotify
  ON catalog_tracks(spotify_track_id);

CREATE INDEX IF NOT EXISTS idx_tracks_isrc
  ON tracks(isrc);

CREATE INDEX IF NOT EXISTS idx_audio_features_recc_uuid
  ON audio_features(reccobeats_track_uuid);

-- Pipeline run log. One row per user per pipeline execution.
-- Used to distinguish "new this run" vs "already in catalog" and to track run history.
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  catalog_tracks_added INTEGER DEFAULT 0,
  recommendations_written INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_user
  ON pipeline_runs (user_id, started_at DESC);

-- Ranked recommendations written by cluster.py after each pipeline run.
-- One row per (user, track). Re-running cluster.py for a user replaces that user's rows.
CREATE TABLE IF NOT EXISTS ranked_recommendations (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,
  name TEXT,
  primary_artist TEXT,
  final_score DOUBLE PRECISION NOT NULL,
  cluster_similarity DOUBLE PRECISION,
  context_score DOUBLE PRECISION,
  rank_position INTEGER NOT NULL,
  pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, spotify_track_id)
);

CREATE INDEX IF NOT EXISTS idx_ranked_recommendations_user_score
  ON ranked_recommendations (user_id, final_score DESC);

-- Weather context captured each pipeline run.
-- cluster.py reads the latest row to optionally rerank candidates.
CREATE TABLE IF NOT EXISTS context_inputs (
  id BIGSERIAL PRIMARY KEY,
  temperature_c DOUBLE PRECISION,
  relative_humidity DOUBLE PRECISION,
  wind_speed_m_s DOUBLE PRECISION,
  text_description TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  observation_time TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_context_inputs_fetched_at
  ON context_inputs (fetched_at DESC);

-- Recently played tracks per user (timestamped, cumulative — never upserted away).
-- Sourced from Spotify /v1/me/player/recently-played (last 50 per pull).
CREATE TABLE IF NOT EXISTS user_recently_played (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,
  played_at TIMESTAMPTZ NOT NULL,
  pulled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, spotify_track_id, played_at)
);

CREATE INDEX IF NOT EXISTS idx_recently_played_user_time
  ON user_recently_played (user_id, played_at DESC);

-- Point-in-time snapshots of a user's top-track rankings.
-- Unlike user_top_tracks (which is an upsert/current-view), this table
-- accumulates one row per (user, track, time_range, snapshot_date) so we
-- can chart how taste drifts over weeks / months.
CREATE TABLE IF NOT EXISTS user_top_track_snapshots (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,
  time_range TEXT CHECK (time_range IN ('short_term','medium_term','long_term')),
  rank INTEGER NOT NULL,
  snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
  pulled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_unique
  ON user_top_track_snapshots (user_id, spotify_track_id, time_range, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_snapshots_user_date
  ON user_top_track_snapshots (user_id, snapshot_date DESC);
