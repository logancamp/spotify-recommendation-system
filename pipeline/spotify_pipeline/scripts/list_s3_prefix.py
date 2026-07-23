import os
import boto3
from dotenv import load_dotenv

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

prefix = "raw/spotify/top_tracks/"
resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

print("Prefix:", prefix)
for obj in resp.get("Contents", []):
    print("-", obj["Key"], obj["Size"])
