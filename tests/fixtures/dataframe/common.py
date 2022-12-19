"""Fixture for testing DataFrameWrapper methods."""
from pyspark.sql import SparkSession
from pytest import fixture


@fixture
def spark_session() -> SparkSession:
    """Local Spark context."""
    return (
        SparkSession.builder.master(
            "local",
        )
        .appName(
            "Test context",
        )
        .getOrCreate()
    )


@fixture
def expected_empty_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None
