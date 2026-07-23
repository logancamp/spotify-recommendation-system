import requests
import json

BASE_URL = "https://api.reccobeats.com"
URL = f"{BASE_URL}/v1/audio-features"

# 3 Spotify track IDs (from your earlier Spotify Search output)
spotify_ids = [
    "18vXApRmJSgQ6wG2ll9AOg",  # RAPSTAR — Polo G
    "7ytR5pFWmSjzHJIeQkgog4",  # ROCKSTAR — DaBaby
    "30UFKKWSOC2Xr6KfWcyvsI",  # ROCKSTAR — DaBaby (alt)
]

ids_param = ",".join(spotify_ids)

r = requests.get(
    URL,
    headers={"Accept": "application/json"},
    params={"ids": ids_param},
    timeout=20
)

print("URL:", URL)
print("ids:", ids_param)
print("Status:", r.status_code)
print("Content-Type:", r.headers.get("Content-Type"))

# Print a preview
text_preview = (r.text[:800] if r.text else "<empty>")
print("Body preview:", text_preview)

# If JSON, summarize what we got
try:
    data = r.json()
    content = data.get("content", [])
    print("\nReturned items:", len(content))
    for i, item in enumerate(content[:3], start=1):
        print(f"{i}. reccobeats_id={item.get('id')}  href={item.get('href')}  tempo={item.get('tempo')}")
except Exception as e:
    print("JSON parse error:", repr(e))
