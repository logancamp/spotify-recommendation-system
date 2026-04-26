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

-- Ranked recommendations written by cluster.py after each pipeline run.
-- One row per (user, track). Re-running cluster.py for a user replaces that user's rows.
CREATE TABLE IF NOT EXISTS ranked_recommendations (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE CASCADE,
  name TEXT,
  primary_artist TEXT,
  final_score DOUBLE PRECISION NOT NULL,
  rank_position INTEGER NOT NULL,
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
