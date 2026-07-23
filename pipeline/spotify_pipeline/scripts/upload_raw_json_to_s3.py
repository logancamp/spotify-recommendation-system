import os
import boto3
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=region,
)

s3 = session.client("s3")

local_path = "data/raw/test_top_tracks.json"
ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
key = f"raw/spotify/top_tracks/test_top_tracks_{ts}.json"

s3.upload_file(
    Filename=local_path,
    Bucket=bucket,
    Key=key,
    ExtraArgs={"ContentType": "application/json"},
)

print("✅ Uploaded to S3")
print("Bucket:", bucket)
print("Key:", key)
print("S3 URL:", f"https://{bucket}.s3.amazonaws.com/{key}")
