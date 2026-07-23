import os
import base64
import requests
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET in .env")

# 1) Get an app-only access token (Client Credentials flow)
token_url = "https://accounts.spotify.com/api/token"
auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

token_resp = requests.post(
    token_url,
    headers={"Authorization": f"Basic {auth_header}"},
    data={"grant_type": "client_credentials"},
    timeout=15
)
token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]

# 2) Search for a track (edit this query)
query = "RAPSTAR"
search_url = f"https://api.spotify.com/v1/search?q={quote(query)}&type=track&limit=5"

search_resp = requests.get(
    search_url,
    headers={"Authorization": f"Bearer {access_token}"},
    timeout=15
)
search_resp.raise_for_status()
items = search_resp.json()["tracks"]["items"]

print(f"Top results for: {query}\n")
for i, t in enumerate(items, start=1):
    track_id = t["id"]
    name = t["name"]
    artist = t["artists"][0]["name"] if t.get("artists") else "Unknown"
    url = t["external_urls"]["spotify"]
    print(f"{i}. {name} — {artist}")
    print(f"   track_id: {track_id}")
    print(f"   url: {url}\n")
