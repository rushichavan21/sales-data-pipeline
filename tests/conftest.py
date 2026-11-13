"""Pytest fixtures for Sales Data Pipeline."""

import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a lightweight local SparkSession for unit testing."""
    if sys.platform == "win32":
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        hadoop_dir = os.path.join(project_root, "hadoop")
        if os.path.exists(hadoop_dir):
            os.environ["HADOOP_HOME"] = hadoop_dir
            bin_path = os.path.join(hadoop_dir, "bin")
            if bin_path not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_path};{os.environ.get('PATH', '')}"
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark_session = (
        SparkSession.builder.appName("SalesPipeline-Tests")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()
