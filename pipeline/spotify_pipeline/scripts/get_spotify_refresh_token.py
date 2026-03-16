import os
import base64
import requests
from flask import Flask, request
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")  # must be http://127.0.0.1:8888/callback

if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
    raise SystemExit("Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET / SPOTIFY_REDIRECT_URI in .env")

SCOPES = [
    "user-top-read",
    "playlist-modify-private",
    "playlist-modify-public",
]

app = Flask(__name__)

@app.route("/callback")
def callback():
    err = request.args.get("error")
    if err:
        return f"Authorization failed: {err}", 400

    code = request.args.get("code")
    if not code:
        return "Missing code parameter.", 400

    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        timeout=20
    )

    if resp.status_code != 200:
        return f"Token exchange failed ({resp.status_code}): {resp.text}", 400

    data = resp.json()
    refresh_token = data.get("refresh_token")
    access_token = data.get("access_token")

    if not refresh_token:
        return "No refresh_token returned. Check app settings and scopes.", 400

    # Print to terminal (safe-ish, but don't share screenshots publicly)
    print("\n✅ SPOTIFY_REFRESH_TOKEN (copy into your .env):\n")
    print(refresh_token)
    print("\n(Access token returned too, but refresh token is what you need long-term.)\n")

    return (
        "<h2>Success!</h2>"
        "<p>Refresh token printed in your terminal.</p>"
        "<p>Copy it into your <code>.env</code> as <code>SPOTIFY_REFRESH_TOKEN=...</code></p>"
        "<p>You can close this tab now.</p>"
    )

def main():
    auth_url = "https://accounts.spotify.com/authorize"
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": " ".join(SCOPES),
        "show_dialog": "true",
    }

    print("\n1) Open this URL in your VM browser and approve access:\n")
    print(auth_url + "?" + urlencode(params))
    print("\n2) After approving, you will be redirected to /callback and this script will print the refresh token.\n")
    print("Listening on 127.0.0.1:8888 ...\n")

    app.run(host="127.0.0.1", port=8888, debug=False)

if __name__ == "__main__":
    main()
