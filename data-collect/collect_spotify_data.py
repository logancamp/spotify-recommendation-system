import csv
import os
import time
import requests
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

CLIENT_ID = "5affff20c0b74e7aa83277b4171ce8a7"
CLIENT_SECRET = "bfe1a82b64f64a2bbc4847a303373de6"
REDIRECT_URI = "http://127.0.0.1:8888/callback"
DATA_DIR = os.path.join(os.getcwd(), "data")

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 8
BASE_RETRY_DELAY_SECONDS = 2
PAGE_DELAY_SECONDS = 0.1
CHECKPOINT_EVERY_N_ROWS = 500

auth_code = None

track_fieldnames = [
    "track_id",
    "track_name",
    "artist_names",
    "track_type",
    "track_uri",
    "track_href",
    "spotify_url",
    "duration_ms",
    "popularity",
    "explicit",
    "track_number",
]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        q = parse_qs(urlparse(self.path).query)
        auth_code = q.get("code", [None])[0]

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"You can close this window")


def spotify_get(url, token, params=None):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                wait_seconds = int(response.headers.get("Retry-After", BASE_RETRY_DELAY_SECONDS))
                print(f"Rate limited. Waiting {wait_seconds} seconds...")
                time.sleep(wait_seconds)
                continue

            if response.status_code in (502, 503, 504):
                wait_seconds = min(BASE_RETRY_DELAY_SECONDS * (2 ** attempt), 30)
                print(f"Spotify server error {response.status_code}. Retrying in {wait_seconds} seconds...")
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            wait_seconds = min(BASE_RETRY_DELAY_SECONDS * (2 ** attempt), 30)
            print(f"Request timed out. Retrying in {wait_seconds} seconds...")
            time.sleep(wait_seconds)

        except requests.exceptions.RequestException as e:
            wait_seconds = min(BASE_RETRY_DELAY_SECONDS * (2 ** attempt), 30)
            print(f"Request failed ({attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(wait_seconds)

    raise Exception(f"Failed after {MAX_RETRIES} retries: {url}")

def write_csv(filename, rows, fieldnames):
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def flatten_track(track):
    artists = track.get("artists", [])
    # Turn the track object into a flat dictionary with relevant fields for output
    return {
        "track_id": track.get("id"),
        "track_name": track.get("name"),
        "artist_names": ", ".join(artist.get("name", "") for artist in artists),
        "track_type": track.get("type"),
        "track_uri": track.get("uri"),
        "track_href": track.get("href"),
        "spotify_url": track.get("external_urls", {}).get("spotify"),
        "duration_ms": track.get("duration_ms"),
        "popularity": track.get("popularity"),
        "explicit": track.get("explicit"),
        "track_number": track.get("track_number"),
    }

def get_all_saved_tracks(token):
    rows = []
    offset = 0
    limit = 50

    while True:
        # Spotify's API call
        data = spotify_get(
            "https://api.spotify.com/v1/me/tracks",
            token,
            params={"limit": limit, "offset": offset},
        )

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            track = item.get("track", {})
            row = flatten_track(track)
            row["added_at"] = item.get("added_at")
            rows.append(row)

        offset += limit
        print(f"Fetched saved tracks: {len(rows)} / {data.get('total', len(rows))}")

        # Stop if we've fetched all tracks
        if offset >= data.get("total", 0):
            break

        time.sleep(PAGE_DELAY_SECONDS)

    return rows


def get_recently_played(token):
    rows = []
    # Spotify's API call
    data = spotify_get(
        "https://api.spotify.com/v1/me/player/recently-played",
        token,
        params={"limit": 50},
    )

    for item in data.get("items", []):
        track = item.get("track", {})
        row = flatten_track(track)
        row["played_at"] = item.get("played_at")
        rows.append(row)

    return rows


def get_top_tracks(token, time_range):
    rows = []
    # Spotify's API call
    data = spotify_get(
        "https://api.spotify.com/v1/me/top/tracks",
        token,
        params={"limit": 50, "time_range": time_range},
    )

    for i, track in enumerate(data.get("items", []), start=1):
        row = flatten_track(track)
        row["rank"] = i
        row["time_range"] = time_range
        rows.append(row)

    return rows


if __name__ == "__main__":
    # Start the local server to handle the OAuth callback (this allows us to open the login page for OAuth)
    server = HTTPServer(("localhost", 8888), Handler)

    # Direct user to Spotify login/authorization
    auth_url = (
        "https://accounts.spotify.com/authorize?"
        f"client_id={CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=user-read-private user-library-read user-read-recently-played user-top-read"
    )
    print(f"Login URL: {auth_url}")
    
    # Open the authorization URL in the user's default web browser
    webbrowser.open(auth_url)
    while auth_code is None:
        server.handle_request()

    # Exchange the authorization code for an access token
    token_response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    token_response.raise_for_status()
    token = token_response.json()["access_token"]

    # Fetch data and write to CSV files
    saved_tracks = get_all_saved_tracks(token)
    recent_tracks = get_recently_played(token)
    top_tracks = get_top_tracks(token, "long_term")

    write_csv(
        "saved_tracks.csv",
        saved_tracks,
        track_fieldnames + ["added_at"],
    )

    write_csv(
        "recent_tracks.csv",
        recent_tracks,
        track_fieldnames + ["played_at"],
    )

    write_csv(
        "top_tracks.csv",
        top_tracks,
        track_fieldnames + ["rank", "time_range"],
    )

    print(f"Saved saved_tracks.csv ({len(saved_tracks)} rows)")
    print(f"Saved recent_tracks.csv ({len(recent_tracks)} rows)")
    print(f"Saved top_tracks.csv ({len(top_tracks)} rows)")