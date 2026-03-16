import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
    raise SystemExit("Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET / SPOTIFY_REFRESH_TOKEN in .env")

token_url = "https://accounts.spotify.com/api/token"
auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

resp = requests.post(
    token_url,
    headers={"Authorization": f"Basic {auth_header}"},
    data={
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    },
    timeout=20
)

print("Status:", resp.status_code)
if resp.status_code != 200:
    print("Body:", resp.text[:400])
    raise SystemExit("❌ Refresh failed")

data = resp.json()
print("✅ Refresh token works. Access token received.")
print("expires_in:", data.get("expires_in"))
print("token_type:", data.get("token_type"))
print("access_token_preview:", data.get("access_token", "")[:10] + "...")
