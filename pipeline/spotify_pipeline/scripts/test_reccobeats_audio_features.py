import json
import requests
from datetime import datetime

BASE_URL = "https://api.reccobeats.com"
SPOTIFY_TRACK_ID = "18vXApRmJSgQ6wG2ll9AOg"  # your RAPSTAR result

url = f"{BASE_URL}/v1/track/{SPOTIFY_TRACK_ID}/audio-features"

r = requests.get(url, timeout=20)

print("URL:", url)
print("Status:", r.status_code)
print("Content-Type:", r.headers.get("Content-Type"))
print("Body preview:", (r.text[:300] if r.text else ""))

# Save response for debugging / reproducibility
out = {
    "tested_at": datetime.utcnow().isoformat() + "Z",
    "url": url,
    "status": r.status_code,
    "headers": dict(r.headers),
    "body": None,
}

try:
    out["body"] = r.json()
except Exception:
    out["body"] = r.text

import os
os.makedirs("data/reccobeats_tests", exist_ok=True)
fname = f"data/reccobeats_tests/reccobeats_track_audio_features_{SPOTIFY_TRACK_ID}.json"
with open(fname, "w") as f:
    json.dump(out, f, indent=2)

print("Saved to:", fname)
