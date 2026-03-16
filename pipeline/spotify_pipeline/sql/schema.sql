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
  PRIMARY KEY (user_id, spotify_track_id, time_range, pulled_at)
);

CREATE TABLE IF NOT EXISTS catalog_tracks (
  catalog_id SERIAL PRIMARY KEY,
  spotify_track_id TEXT REFERENCES tracks(spotify_track_id) ON DELETE SET NULL,
  source TEXT,                 -- from the documentations:  'spotify_search', 'curated_list'
  query_used TEXT,             -- from the documentation: the search query/label used to collect it
  pulled_at TIMESTAMPTZ DEFAULT NOW(),
  raw_json JSONB
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
