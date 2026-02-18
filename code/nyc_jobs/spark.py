"""Spark session builder."""

from pyspark.sql import SparkSession

from nyc_jobs.config import APP_NAME, SPARK_MASTER


def create_spark_session(use_cluster_master: bool = False) -> SparkSession:
    """Create and return a SparkSession."""
    builder = SparkSession.builder.appName(APP_NAME)
    if use_cluster_master:
        builder = builder.master(SPARK_MASTER)
    return builder.getOrCreate()

