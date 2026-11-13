"""AWS Glue Job Deployer, Runner, and Cost Estimator.

Adheres strictly to project cost and safety rules:
- Pre-calculates and displays estimated DPU costs before running
- Configures minimum worker count (2 workers, G.1X)
- Uses FLEX execution class to minimize costs (~$0.29/DPU-hr)
- Least-privilege configuration with short execution timeout (10 mins)
- Separate curated-glue/ prefix to preserve baseline curated/ data
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
    dpus = num_workers  # 2 G.1X workers = 2 DPUs
    rate = DPU_HOURLY_RATE_FLEX if is_flex else DPU_HOURLY_RATE_STANDARD
    hours = expected_duration_minutes / 60.0
    est_cost = dpus * rate * hours
    return round(est_cost, 4)


def get_glue_client(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    return boto3.client(
        "glue",
        region_name=region_name,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def get_s3_client(region_name: str = "ap-south-1", profile_name: Optional[str] = None):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def upload_glue_script(s3_client, bucket_name: str, local_script: str = "glue_jobs/clean_sales.py") -> str:
    """Upload Glue script to S3."""
    s3_key = "scripts/clean_sales.py"
    logger.info("Uploading %s to s3://%s/%s...", local_script, bucket_name, s3_key)
    s3_client.upload_file(local_script, bucket_name, s3_key)
    script_location = f"s3://{bucket_name}/{s3_key}"
    logger.info("Glue script uploaded successfully to: %s", script_location)
    return script_location


def create_or_update_glue_job(
    glue_client,
    job_name: str,
    role_arn: str,
    script_s3_location: str,
    bucket_name: str,
    output_prefix: str = "curated-glue/sales_curated",
    use_flex: bool = True,
):
    """Create or update AWS Glue Spark job with minimal worker configuration."""
    num_workers = 2
    worker_type = "G.1X"
    execution_class = "FLEX" if use_flex else "STANDARD"

    input_location = f"s3://{bucket_name}/raw/superstore_sales_raw.csv"
    output_location = f"s3://{bucket_name}/{output_prefix}"

    est_cost = estimate_glue_cost(num_workers=num_workers, expected_duration_minutes=3.0, is_flex=use_flex)
    
    print("\n=========================================================")
    print("           AWS GLUE JOB DEPLOYMENT DETAILS              ")
    print("=========================================================")
    print(f"Job Name:            {job_name}")
    print(f"IAM Role ARN:        {role_arn}")
    print(f"Worker Count:        {num_workers} workers")
    print(f"Worker Type:         {worker_type} (1 DPU per worker = 2 DPUs total)")
    print(f"Execution Class:     {execution_class} (~$0.29 / DPU-hour)")
    print(f"Glue Version:        4.0 (Apache Spark 3.3, Python 3.10)")
    print(f"Max Safety Timeout:  10 minutes")
    print(f"Script S3 Location:  {script_s3_location}")
    print(f"Input Location:      {input_location}")
    print(f"Output Location:     {output_location}")
    print("---------------------------------------------------------")
    print(f"COST ESTIMATE:")
    print(f"Estimated Duration:  ~2 to 3 minutes")
    print(f"Estimated Cost:      ~${est_cost:.4f} USD (well within $5 budget alert)")
    print("=========================================================\n")

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
            "--input_path": input_location,
            "--output_path": output_location,
        },
        "MaxRetries": 0,
        "Timeout": 10,
        "GlueVersion": "4.0",
        "NumberOfWorkers": num_workers,
        "WorkerType": worker_type,
        "ExecutionClass": execution_class,
    }

    try:
        glue_client.get_job(JobName=job_name)
        logger.info("Updating existing Glue job definition: %s", job_name)
        glue_client.update_job(JobName=job_name, JobUpdate=job_kwargs)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
            logger.info("Creating new Glue job: %s", job_name)
            glue_client.create_job(**job_kwargs)
        else:
            raise

    logger.info("Glue job configured and ready: %s", job_name)


def trigger_glue_job_run(glue_client, job_name: str, bucket_name: str, output_prefix: str = "curated-glue/sales_curated"):
    """Trigger Glue job run and monitor execution once."""
    input_location = f"s3://{bucket_name}/raw/superstore_sales_raw.csv"
    output_location = f"s3://{bucket_name}/{output_prefix}"

    logger.info("Triggering run for job '%s'...", job_name)
    logger.info("Writing curated output to: %s", output_location)

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                "--input_path": input_location,
                "--output_path": output_location,
            },
        )
    except ClientError as e:
        logger.error("Failed to start Glue job run: %s", e)
        raise

    job_run_id = response["JobRunId"]
    logger.info("Job run started with Run ID: %s", job_run_id)

    # Monitor run until terminal state
    start_time = time.time()
    while True:
        status_res = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        run_info = status_res["JobRun"]
        run_state = run_info["JobRunState"]
        elapsed = round(time.time() - start_time)
        logger.info("Job status [%ds]: %s", elapsed, run_state)

        if run_state in ("SUCCEEDED", "STOPPED", "FAILED", "TIMEOUT", "ERROR"):
            if run_state == "SUCCEEDED":
                dpu_seconds = run_info.get("DPUSeconds", 0)
                actual_cost = round((dpu_seconds / 3600.0) * DPU_HOURLY_RATE_FLEX, 4)
                print("\n=========================================================")
                print("                 GLUE JOB SUCCEEDED!                     ")
                print("=========================================================")
                print(f"Job Run ID:       {job_run_id}")
                print(f"Total Time:       {elapsed} seconds")
                print(f"DPU Seconds:      {dpu_seconds}")
                print(f"Actual Cost:      ~${actual_cost:.4f} USD")
                print(f"Output Location:  {output_location}")
                print("=========================================================\n")
            else:
                err_msg = run_info.get("ErrorMessage", "Unknown error")
                print("\n=========================================================")
                print("                   GLUE JOB FAILED!                      ")
                print("=========================================================")
                print(f"State:            {run_state}")
                print(f"Error Message:    {err_msg}")
                print("=========================================================\n")
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
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME", "sales-pipeline-rushi-2026-unique"))
    parser.add_argument("--role-arn", default=os.getenv("GLUE_ROLE_ARN"))
    parser.add_argument("--output-prefix", default="curated-glue/sales_curated", help="S3 prefix for Glue output")
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
        create_or_update_glue_job(
            glue_client,
            job_name=args.job_name,
            role_arn=args.role_arn,
            script_s3_location=script_loc,
            bucket_name=args.bucket,
            output_prefix=args.output_prefix,
        )

    if args.run:
        trigger_glue_job_run(
            glue_client,
            job_name=args.job_name,
            bucket_name=args.bucket,
            output_prefix=args.output_prefix,
        )


if __name__ == "__main__":
    main()
