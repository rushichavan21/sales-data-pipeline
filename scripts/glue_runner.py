"""AWS Glue Job Deployer, Runner, and Cost Estimator.

Adheres strictly to project cost and safety rules:
- Pre-calculates and displays estimated DPU costs before running
- Configures minimum worker count (2 workers, G.1X)
- Uses FLEX execution class where available to minimize costs
- Least-privilege configuration with short execution timeout (10 mins)
- Includes teardown capability
"""

import argparse
import logging
import os
import sys
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GlueRunner")

# AWS Glue pricing in ap-south-1 (Mumbai): ~$0.44 per DPU-Hour for Standard, ~$0.29 for Flex
DPU_HOURLY_RATE_STANDARD = 0.44
DPU_HOURLY_RATE_FLEX = 0.29


def estimate_glue_cost(num_workers: int = 2, expected_duration_minutes: float = 3.0, is_flex: bool = True) -> float:
    """Calculate and display upfront estimated Glue job cost."""
    # 2 G.1X workers = 2 DPUs. Glue 3.0+ minimum billable execution time is 1 minute.
    dpus = num_workers
    rate = DPU_HOURLY_RATE_FLEX if is_flex else DPU_HOURLY_RATE_STANDARD
    hours = expected_duration_minutes / 60.0
    est_cost = dpus * rate * hours
    return round(est_cost, 4)


def get_glue_client(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.client("glue")


def get_s3_client(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.client("s3")


def upload_glue_script(s3_client, bucket_name: str, local_script: str = "glue_jobs/clean_sales.py") -> str:
    """Upload Glue script to S3."""
    s3_key = "scripts/clean_sales.py"
    logger.info("Uploading %s to s3://%s/%s...", local_script, bucket_name, s3_key)
    s3_client.upload_file(local_script, bucket_name, s3_key)
    script_location = f"s3://{bucket_name}/{s3_key}"
    logger.info("Glue script ready at: %s", script_location)
    return script_location


def create_or_update_glue_job(
    glue_client,
    job_name: str,
    role_arn: str,
    script_s3_location: str,
    bucket_name: str,
    use_flex: bool = True,
):
    """Create or update AWS Glue Spark job with minimal worker configuration."""
    num_workers = 2
    worker_type = "G.1X"
    execution_class = "FLEX" if use_flex else "STANDARD"

    est_cost = estimate_glue_cost(num_workers=num_workers, expected_duration_minutes=3.0, is_flex=use_flex)
    logger.info("---------------------------------------------------------")
    logger.info("GLUE COST ESTIMATE:")
    logger.info("Workers: %d x %s (2 DPUs)", num_workers, worker_type)
    logger.info("Execution Class: %s", execution_class)
    logger.info("Estimated runtime: ~2-3 minutes")
    logger.info("Estimated cost for this run: ~$%.4f USD (well within $10 budget)", est_cost)
    logger.info("---------------------------------------------------------")

    job_kwargs = {
        "Name": job_name,
        "Role": role_arn,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--TempDir": f"s3://{bucket_name}/temp/",
            "--spark-event-logs-path": f"s3://{bucket_name}/spark-events/",
            "--input_path": f"s3://{bucket_name}/raw/superstore_sales_raw.csv",
            "--output_path": f"s3://{bucket_name}/curated/sales_curated",
        },
        "MaxRetries": 0,
        "Timeout": 10,  # 10 minutes max safety timeout
        "GlueVersion": "4.0",
        "NumberOfWorkers": num_workers,
        "WorkerType": worker_type,
        "ExecutionClass": execution_class,
    }

    try:
        glue_client.get_job(JobName=job_name)
        logger.info("Updating existing Glue job: %s", job_name)
        glue_client.update_job(JobName=job_name, JobUpdate=job_kwargs)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
            logger.info("Creating new Glue job: %s", job_name)
            glue_client.create_job(**job_kwargs)
        else:
            raise

    logger.info("Glue job configured successfully: %s", job_name)


def trigger_glue_job_run(glue_client, job_name: str, bucket_name: str):
    """Trigger Glue job run and monitor execution."""
    logger.info("Triggering run for job '%s'...", job_name)
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            "--input_path": f"s3://{bucket_name}/raw/superstore_sales_raw.csv",
            "--output_path": f"s3://{bucket_name}/curated/sales_curated",
        },
    )
    job_run_id = response["JobRunId"]
    logger.info("Job run started with Run ID: %s", job_run_id)

    # Monitor run
    start_time = time.time()
    while True:
        status_res = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        run_state = status_res["JobRun"]["JobRunState"]
        elapsed = round(time.time() - start_time)
        logger.info("Job status [%ds]: %s", elapsed, run_state)

        if run_state in ("SUCCEEDED", "STOPPED", "FAILED", "TIMEOUT", "ERROR"):
            if run_state == "SUCCEEDED":
                dpu_seconds = status_res["JobRun"].get("DPUSeconds", 0)
                actual_cost = round((dpu_seconds / 3600.0) * DPU_HOURLY_RATE_FLEX, 4)
                logger.info("Job SUCCEEDED! DPU Seconds used: %s, Actual cost: ~$%.4f USD", dpu_seconds, actual_cost)
            else:
                err_msg = status_res["JobRun"].get("ErrorMessage", "Unknown error")
                logger.error("Job ended with state %s: %s", run_state, err_msg)
            break

        time.sleep(15)


def teardown_glue_job(glue_client, job_name: str):
    """Delete the Glue job."""
    try:
        glue_client.delete_job(JobName=job_name)
        logger.info("Successfully deleted Glue job: %s", job_name)
    except ClientError as e:
        logger.warning("Could not delete Glue job: %s", e)


def main():
    parser = argparse.ArgumentParser(description="AWS Glue Job Orchestrator")
    parser.add_argument("--job-name", default="sales-data-cleaner-job")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME"))
    parser.add_argument("--role-arn", default=os.getenv("GLUE_ROLE_ARN"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-1"))
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument("--deploy", action="store_true", help="Upload script and create/update Glue job")
    parser.add_argument("--run", action="store_true", help="Trigger job execution")
    parser.add_argument("--teardown", action="store_true", help="Delete the Glue job")

    args = parser.parse_args()

    if not args.bucket:
        logger.error("Bucket name required. Set S3_BUCKET_NAME in .env or pass --bucket.")
        sys.exit(1)

    glue_client = get_glue_client(region_name=args.region, profile_name=args.profile)
    s3_client = get_s3_client(region_name=args.region, profile_name=args.profile)

    if args.teardown:
        teardown_glue_job(glue_client, args.job_name)
        return

    if args.deploy:
        if not args.role_arn:
            logger.error("Glue IAM Role ARN is required for deploy. Pass --role-arn or set GLUE_ROLE_ARN in .env.")
            sys.exit(1)
        script_loc = upload_glue_script(s3_client, args.bucket)
        create_or_update_glue_job(glue_client, args.job_name, args.role_arn, script_loc, args.bucket)

    if args.run:
        trigger_glue_job_run(glue_client, args.job_name, args.bucket)


if __name__ == "__main__":
    main()
