import os
import base64
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOP_N = 10

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")


def get_spotify_access_token():
    if not all([SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN]):
        raise SystemExit("Missing Spotify OAuth values in .env")

    token_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()

    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": SPOTIFY_REFRESH_TOKEN,
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_user_profile(access_token):
    resp = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def create_playlist(access_token, playlist_name, description):
    url = "https://api.spotify.com/v1/me/playlists"
    payload = {
        "name": playlist_name,
        "description": description,
        "public": False
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )

    if resp.status_code not in (200, 201):
        print("Create playlist failed")
        print("Status:", resp.status_code)
        print("Body:", resp.text)
        resp.raise_for_status()

    return resp.json()


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def add_tracks_to_playlist(access_token, playlist_id, track_uris):
    if not track_uris:
        raise SystemExit("No track URIs provided to add to playlist.")

    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/items"
    snapshot_ids = []

    for batch in chunk_list(track_uris, 100):
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
            },
            params={"uris": ",".join(batch)},
            timeout=20,
        )

        if resp.status_code not in (200, 201):
            print("Add tracks failed")
            print("Status:", resp.status_code)
            print("Body:", resp.text)
            print("Batch URIs:", batch)
            resp.raise_for_status()

        data = resp.json()
        snapshot_ids.append(data.get("snapshot_id"))

    return {"snapshot_ids": snapshot_ids}


def load_top_unique_recommendations(user_hash: str):
    ranked_file = f"data/{user_hash}_candidates_ranked_db.csv"

    if not os.path.exists(ranked_file):
        raise SystemExit(
            f"Ranked file not found for current user: {ranked_file}\n"
            "Run cluster.py first for this user."
        )

    df = pd.read_csv(ranked_file)

    if df.empty:
        raise SystemExit("Ranked file is empty.")

    if "spotify_track_id" not in df.columns:
        raise SystemExit("Expected column 'spotify_track_id' not found in ranked CSV.")

    if "final_score" not in df.columns:
        raise SystemExit("Expected column 'final_score' not found in ranked CSV.")

    df = df.sort_values("final_score", ascending=False).copy()
    df = df.drop_duplicates(subset=["spotify_track_id"], keep="first").copy()

    top_df = df.head(TOP_N).copy()
    top_df["spotify_uri"] = top_df["spotify_track_id"].apply(lambda x: f"spotify:track:{x}")
    return ranked_file, top_df


def main():
    access_token = get_spotify_access_token()
    user = get_current_user_profile(access_token)
    user_hash = user["id"]

    print("Spotify current user:")
    print("id:", user.get("id"))
    print("display_name:", user.get("display_name"))
    print("product:", user.get("product"))

    ranked_file, top_df = load_top_unique_recommendations(user_hash=user_hash)
    print(f"\nUsing ranked file: {ranked_file}")

    playlist = create_playlist(
        access_token=access_token,
        playlist_name="CSDS 417 Recommended Playlist",
        description=f"Generated from ranked recommendations for Spotify user {user_hash}"
    )

    print("\n✅ Playlist created successfully")
    print("playlist_id:", playlist.get("id"))
    print("playlist_name:", playlist.get("name"))
    print("playlist_url:", playlist.get("external_urls", {}).get("spotify"))

    track_uris = top_df["spotify_uri"].tolist()

    add_result = add_tracks_to_playlist(
        access_token=access_token,
        playlist_id=playlist["id"],
        track_uris=track_uris
    )

    print("\n✅ Tracks added to playlist successfully")
    print("snapshot_ids:", add_result.get("snapshot_ids"))

    print("\nFinal tracks added:")
    print(top_df[["spotify_track_id", "name", "primary_artist", "final_score", "spotify_uri"]])


if __name__ == "__main__":
    main()
