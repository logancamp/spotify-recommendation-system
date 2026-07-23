-- Reset data (keep schema)
TRUNCATE TABLE
  audio_features,
  catalog_tracks,
  user_top_tracks,
  tracks,
  users
RESTART IDENTITY CASCADE;
