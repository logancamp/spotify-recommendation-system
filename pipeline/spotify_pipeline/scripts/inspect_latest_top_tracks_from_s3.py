import os
import json
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

prefix = "raw/spotify/top_tracks/tdo18_demo/"
resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

objs = sorted(resp.get("Contents", []), key=lambda x: x["LastModified"], reverse=True)
print("Found objects:", len(objs))
if not objs:
    raise SystemExit("No objects found in S3 under prefix.")

# Show the 3 most recent keys
for o in objs[:3]:
    print("-", o["Key"])

# Read the most recent object and summarize
key = objs[0]["Key"]
data = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
j = json.loads(data)

items = j.get("items", [])
print("\nInspecting:", key)
print("Top-level keys:", list(j.keys()))
print("items length:", len(items))

# If empty, show next/previous keys too (so we can check other time ranges)
if len(items) == 0:
    print("\nitems is empty. Showing 'total' field if present:", j.get("total"))
else:
    # Print first track basic info
    t = items[0]
    print("\nFirst track example:")
    print("id:", t.get("id"))
    print("name:", t.get("name"))
    print("artist:", (t.get("artists")[0].get("name") if t.get("artists") else None))
