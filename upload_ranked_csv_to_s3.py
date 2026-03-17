import os
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv

load_dotenv()

RANKED_FILE = "data/candidates_ranked_db.csv"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET_NAME")

if not BUCKET:
    raise SystemExit("Missing S3_BUCKET_NAME in .env")

if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN]):
    raise SystemExit("Missing AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_SESSION_TOKEN in .env")

if not os.path.exists(RANKED_FILE):
    raise SystemExit(f"Ranked CSV not found: {RANKED_FILE}")

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"reports/recommendations/candidates_ranked_db_{ts}.csv"

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    s3.upload_file(
        RANKED_FILE,
        BUCKET,
        key,
        ExtraArgs={"ContentType": "text/csv"}
    )

    print("✅ Uploaded ranked recommendations CSV to S3")
    print(f"s3://{BUCKET}/{key}")
    print(f"https://{BUCKET}.s3.amazonaws.com/{key}")

if __name__ == "__main__":
    main()
