import os
import base64
import requests
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET_NAME")

if not BUCKET:
    raise SystemExit("Missing S3_BUCKET_NAME in .env")

if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN]):
    raise SystemExit("Missing AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_SESSION_TOKEN in .env")

if not all([SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN]):
    raise SystemExit("Missing Spotify OAuth values in .env")


def get_spotify_access_token() -> str:
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
        data={"grant_type": "refresh_token", "refresh_token": SPOTIFY_REFRESH_TOKEN},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_user_hash() -> str:
    access_token = get_spotify_access_token()
    resp = requests.get(
        "https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def main():
    user_hash = get_current_user_hash()
    safe_user = user_hash.replace("/", "_")
    ranked_file = f"data/{safe_user}_candidates_ranked_db.csv"

    if not os.path.exists(ranked_file):
        raise SystemExit(
            f"Ranked CSV not found: {ranked_file}\n"
            "Run cluster.py first for this user."
        )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"reports/recommendations/{safe_user}_candidates_ranked_db_{ts}.csv"

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    s3.upload_file(
        ranked_file,
        BUCKET,
        key,
        ExtraArgs={"ContentType": "text/csv"}
    )

    print(f"✅ Uploaded ranked recommendations CSV to S3 (user={user_hash})")
    print(f"s3://{BUCKET}/{key}")
    print(f"https://{BUCKET}.s3.amazonaws.com/{key}")


if __name__ == "__main__":
    main()
