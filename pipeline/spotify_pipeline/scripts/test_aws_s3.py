import os
import boto3
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_REGION"),
)

s3 = session.client("s3")
resp = s3.list_buckets()
print("✅ Connected. Buckets visible:", len(resp.get("Buckets", [])))
for b in resp.get("Buckets", [])[:10]:
    print("-", b["Name"])

