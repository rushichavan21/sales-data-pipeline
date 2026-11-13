"""Amazon Athena Query Runner and Performance Analyzer.

Executes DDL, MSCK REPAIR, analytical queries, and captures execution metrics
(Data Scanned in Bytes, Execution Time) for performance benchmarking.
"""

import argparse
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AthenaRunner")


def get_athena_client(region_name: str = "ap-south-1"):
    """Initialize Athena client using credentials in .env."""
    return boto3.client(
        "athena",
        region_name=region_name,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def execute_query(
    client,
    query_string: str,
    database: str = "sales_db",
    output_location: Optional[str] = None,
    wait: bool = True,
) -> Tuple[str, Dict[str, Any]]:
    """Execute SQL query in Athena and return query_execution_id and statistics."""
    if not output_location:
        bucket = os.getenv("S3_BUCKET_NAME", "sales-pipeline-rushi-2026-unique")
        output_location = f"s3://{bucket}/athena-results/"

    logger.info("Submitting query:\n%s", query_string.strip())
    response = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_id = response["QueryExecutionId"]
    logger.info("Query Execution ID: %s", query_id)

    if not wait:
        return query_id, {}

    # Poll for query completion
    start_time = time.time()
    while True:
        status_res = client.get_query_execution(QueryExecutionId=query_id)
        execution = status_res["QueryExecution"]
        state = execution["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            stats = execution.get("Statistics", {})
            data_scanned = stats.get("DataScannedInBytes", 0)
            exec_time_ms = stats.get("TotalExecutionTimeInMillis", 0)

            if state == "SUCCEEDED":
                logger.info(
                    "Query SUCCEEDED in %.2fs | Data Scanned: %s bytes (%.2f KB / %.2f MB)",
                    exec_time_ms / 1000.0,
                    f"{data_scanned:,}",
                    data_scanned / 1024.0,
                    data_scanned / (1024.0 * 1024.0),
                )
                return query_id, {
                    "state": state,
                    "data_scanned_bytes": data_scanned,
                    "exec_time_ms": exec_time_ms,
                    "stats": stats,
                }
            else:
                reason = execution["Status"].get("StateChangeReason", "Unknown error")
                logger.error("Query %s: %s", state, reason)
                raise RuntimeError(f"Athena query {query_id} failed: {reason}")

        time.sleep(1)


def fetch_query_results(client, query_id: str, max_rows: int = 20) -> List[List[str]]:
    """Fetch tabular rows from query results."""
    paginator = client.get_paginator("get_query_results")
    rows = []
    for page in paginator.paginate(QueryExecutionId=query_id):
        result_set = page.get("ResultSet", {})
        page_rows = result_set.get("Rows", [])
        for r in page_rows:
            data = [col.get("VarCharValue", "") for col in r.get("Data", [])]
            rows.append(data)
            if len(rows) > max_rows:
                break
        if len(rows) > max_rows:
            break
    return rows


def print_table(headers: List[str], data_rows: List[List[str]]):
    """Print ASCII formatted table."""
    if not headers and not data_rows:
        print("(No rows returned)")
        return

    all_rows = [headers] + data_rows
    col_widths = [max(len(str(row[i])) for row in all_rows) for i in range(len(headers))]

    header_line = " | ".join(headers[i].ljust(col_widths[i]) for i in range(len(headers)))
    divider_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))

    print("\n" + header_line)
    print(divider_line)
    for row in data_rows:
        print(" | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(headers))))
    print(f"({len(data_rows)} rows shown)\n")


def main():
    parser = argparse.ArgumentParser(description="Athena Query Runner")
    parser.add_argument("--sql", help="Inline SQL query to execute")
    parser.add_argument("--file", help="SQL file path to execute")
    parser.add_argument("--db", default="sales_db", help="Athena database")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-1"))

    args = parser.parse_args()
    client = get_athena_client(region_name=args.region)

    query_sql = None
    if args.sql:
        query_sql = args.sql
    elif args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            query_sql = f.read()
    else:
        print("Specify --sql or --file")
        return

    query_id, metrics = execute_query(client, query_sql, database=args.db)
    results = fetch_query_results(client, query_id)
    if results:
        headers = results[0]
        data_rows = results[1:]
        print_table(headers, data_rows)


if __name__ == "__main__":
    main()
