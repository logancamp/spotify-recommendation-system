import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw_candidate_search")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET_NAME")

if not BUCKET:
    raise SystemExit("Missing S3_BUCKET_NAME in .env")

if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN]):
    raise SystemExit("Missing AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_SESSION_TOKEN in .env")

if not RAW_DIR.exists():
    raise SystemExit(f"Raw candidate search directory not found: {RAW_DIR}")

def main():
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit("No raw candidate search JSON files found.")

    uploaded = 0

    for f in files:
        key = f"raw/spotify/candidate_search/{f.name}"
        s3.upload_file(
            str(f),
            BUCKET,
            key,
            ExtraArgs={"ContentType": "application/json"}
        )
        print(f"✅ Uploaded {f.name} -> s3://{BUCKET}/{key}")
        uploaded += 1

    print(f"\nDone. Uploaded {uploaded} raw candidate-search JSON files.")

if __name__ == "__main__":
    main()
