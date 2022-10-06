from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union

import pytest
from _pytest.fixtures import FixtureRequest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pytest import raises, FixtureRequest, fixture

from sds_data_model.dataframe import DataFrameWrapper

@fixture
def spark_context() -> SparkSession:
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

def expected_name() -> str:
    """Expected DataFrameWrapper name."""
    return "Trial csv"


def expected_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None


def expected_schema() :
    """Expected DataFrameWrapper schema."""
    return StructType([
        StructField("_c0", StringType(), True),
        StructField("_c1", StringType(), True)
])
    
def expected_data():
    data = spark_context.read.csv("tests/data/test_data.csv")
    return data


def test_vector_layer_from_files(
    #shared_datadir: Path,
    expected_name: str,
    expected_schema,
    expected_metadata: None,
    expected_data
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""
    data_path = "tests/data/test_data.csv"
    received = DataFrameWrapper.from_files(
        data_path=data_path,
        name='Trial csv',
    )
    
    assert received.name == expected_name
    assert received.meta == expected_metadata
    assert received.data.schema == expected_schema
    assert_df_equality(received.data, expected_data)