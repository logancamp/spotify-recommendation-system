import os
from dotenv import load_dotenv

load_dotenv()  # loads .env from current directory

keys = [
    "SPOTIFY_CLIENT_ID",
    "SPOTIFY_CLIENT_SECRET",
    "SPOTIFY_REDIRECT_URI",
]

print("Loaded .env:", "YES" if any(os.getenv(k) for k in keys) else "NO")

for k in keys:
    v = os.getenv(k)
    if not v:
        print(f"{k}: MISSING")
    else:
        # print only a tiny safe preview
        preview = v[:4] + "..." + v[-4:] if len(v) >= 10 else "SET"
        print(f"{k}: SET ({preview})")

# extra check: redirect URI should use 127.0.0.1, not localhost
ru = os.getenv("SPOTIFY_REDIRECT_URI", "")
if "localhost" in ru:
    print("WARNING: Redirect URI uses localhost. Spotify may reject it. Use 127.0.0.1.")
else:
    print("Redirect URI looks ok.")
