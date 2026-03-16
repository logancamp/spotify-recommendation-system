import os
import json
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

load_dotenv()

# DB
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_pre_ping=True)

# AWS S3
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_REGION"),
)
s3 = session.client("s3")
bucket = os.getenv("S3_BUCKET_NAME")

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT u.spotify_track_id, u.time_range, u.rank
        FROM user_top_tracks u
        LEFT JOIN audio_features a ON u.spotify_track_id = a.spotify_track_id
        WHERE a.spotify_track_id IS NULL
        ORDER BY u.time_range, u.rank
    """)).fetchall()

missing = [{"spotify_track_id": r[0], "time_range": r[1], "rank": r[2]} for r in rows]

report = {
    "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "missing_count": len(missing),
    "missing": missing,
    "note": "Tracks present in user_top_tracks but missing from ReccoBeats /v1/audio-features results."
}

key = f"reports/data_quality/missing_audio_features_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(report, indent=2).encode("utf-8"),
    ContentType="application/json",
)

print("✅ Uploaded missing-features report to S3:")
print(f"s3://{bucket}/{key}")
print(f"https://{bucket}.s3.amazonaws.com/{key}")
