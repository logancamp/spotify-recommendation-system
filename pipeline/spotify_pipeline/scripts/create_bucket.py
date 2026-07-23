import os
import boto3
from botocore.exceptions import ClientError
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

s3 = session.client("s3", region_name=region)

try:
    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    print("✅ Created bucket:", bucket)
except ClientError as e:
    print("Bucket create error:", e)
