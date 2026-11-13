"""S3 Storage Manager for Sales Data Pipeline.

Handles:
- S3 Bucket creation in the configured AWS region (default: ap-south-1)
- Folder structure initialization (raw/, curated/, athena-results/)
- Upload of raw CSV and curated partitioned Parquet files
- Safe teardown / deletion of created S3 assets to avoid lingering costs
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("S3Manager")


def get_s3_client(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    """Create S3 client with optional profile and region."""
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.client("s3")


def get_s3_resource(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    """Create S3 resource with optional profile and region."""
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.resource("s3")


def bucket_exists(s3_client, bucket_name: str) -> bool:
    """Check if the S3 bucket already exists and is accessible."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchBucket"):
            return False
        elif error_code == "403":
            logger.warning("Bucket %s exists but you do not have permission to access it.", bucket_name)
            return False
        else:
            logger.error("Error checking bucket %s: %s", bucket_name, e)
            return False


def create_bucket(s3_client, bucket_name: str, region_name: str = "ap-south-1") -> bool:
    """Create an S3 bucket with appropriate region constraint."""
    if bucket_exists(s3_client, bucket_name):
        logger.info("Bucket '%s' already exists and is accessible.", bucket_name)
        return True

    logger.info("Creating bucket '%s' in region '%s'...", bucket_name, region_name)
    try:
        if region_name == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {"LocationConstraint": region_name}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
            )
        logger.info("Successfully created bucket: %s", bucket_name)
        return True
    except ClientError as e:
        logger.error("Failed to create bucket '%s': %s", bucket_name, e)
        raise


def init_s3_folders(s3_client, bucket_name: str):
    """Initialize logical folders in the S3 bucket."""
    prefixes = ["raw/", "curated/", "athena-results/"]
    for prefix in prefixes:
        s3_client.put_object(Bucket=bucket_name, Key=prefix)
        logger.info("Ensured S3 folder prefix: s3://%s/%s", bucket_name, prefix)


def upload_raw_data(s3_client, bucket_name: str, local_path: str = "data/raw/superstore_sales_raw.csv"):
    """Upload raw sales CSV to S3."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local raw data not found at {local_path}. Run generate_sales_dataset.py first.")

    s3_key = "raw/superstore_sales_raw.csv"
    logger.info("Uploading %s to s3://%s/%s...", local_path, bucket_name, s3_key)
    s3_client.upload_file(local_path, bucket_name, s3_key)
    logger.info("Raw data uploaded successfully.")


def upload_curated_parquet(s3_client, bucket_name: str, local_curated_dir: str = "data/curated/sales_curated"):
    """Recursively upload partitioned Parquet files preserving partition directory structure."""
    curated_path = Path(local_curated_dir)
    if not curated_path.exists():
        raise FileNotFoundError(f"Curated directory {local_curated_dir} does not exist. Run PySpark job first.")

    files_uploaded = 0
    logger.info("Uploading curated partitioned Parquet files from %s...", local_curated_dir)
    for root, _, files in os.walk(curated_path):
        for file in files:
            # Skip hidden spark crc files and temp files
            if file.startswith(".") or file.endswith(".crc"):
                continue

            full_local_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_local_path, start=local_curated_dir)
            s3_key = f"curated/sales_curated/{relative_path}".replace("\\", "/")

            s3_client.upload_file(full_local_path, bucket_name, s3_key)
            files_uploaded += 1

    logger.info("Uploaded %d curated Parquet files to s3://%s/curated/sales_curated/", files_uploaded, bucket_name)


def teardown_s3(s3_resource, bucket_name: str):
    """Safely delete all objects and drop the bucket for project teardown."""
    logger.warning("Starting teardown for bucket: %s...", bucket_name)
    try:
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()
        logger.info("Successfully deleted bucket and all objects: %s", bucket_name)
    except ClientError as e:
        logger.error("Teardown error: %s", e)


def main():
    parser = argparse.ArgumentParser(description="S3 Storage Manager for Sales Data Pipeline")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME"), help="S3 bucket name")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-1"), help="AWS region")
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"), help="AWS profile name")
    parser.add_argument("--upload-all", action="store_true", help="Create bucket and upload all raw and curated data")
    parser.add_argument("--teardown", action="store_true", help="Delete all objects and remove the bucket")

    args = parser.parse_args()

    if not args.bucket:
        logger.error("Bucket name is required. Specify via --bucket or S3_BUCKET_NAME in .env file.")
        sys.exit(1)

    s3_client = get_s3_client(region_name=args.region, profile_name=args.profile)
    s3_resource = get_s3_resource(region_name=args.region, profile_name=args.profile)

    if args.teardown:
        confirm = input(f"Are you sure you want to permanently delete s3://{args.bucket}? (yes/no): ")
        if confirm.strip().lower() == "yes":
            teardown_s3(s3_resource, args.bucket)
        else:
            logger.info("Teardown cancelled.")
        return

    if args.upload_all:
        create_bucket(s3_client, args.bucket, region_name=args.region)
        init_s3_folders(s3_client, args.bucket)
        upload_raw_data(s3_client, args.bucket)
        upload_curated_parquet(s3_client, args.bucket)
        logger.info("Phase 2 S3 setup and upload complete for s3://%s", args.bucket)


if __name__ == "__main__":
    main()
