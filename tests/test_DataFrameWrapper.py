from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union
import os

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pytest import fixture
from pandas import DataFrame

from sds_data_model.dataframe import DataFrameWrapper

# expected_data = {'a':[1,2,3],
#        'b':[3,4,5]}
# expected_df= DataFrame(expected_data)


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

# @fixture
# def test_create_file(tmpdir):
#     p = tmpdir.join("temp.csv")
#     return p
#     #write somethign to temp directory and return its name
#     # return p (path)

@fixture
def expected_name() -> str:
    """Expected DataFrameWrapper name."""
    return "Trial csv"

@fixture
def expected_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None

@fixture
def expected_schema():
    """Expected DataFrameWrapper schema."""
    return StructType([
        StructField("_c0", StringType(), True),
        StructField("_c1", StringType(), True)
    ])

@fixture
def expected_data(spark_context):
    data = spark_context.read.csv("tests/data/test_data.csv")
    return data


def test_vector_layer_from_files(
    spark_context,
    #shared_datadir: Path,
    #datapath fixture
    expected_name: str,
    expected_schema,
    expected_metadata: None,
    expected_data
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""
    data_path = "tests/data/test_data.csv"
    #data_path = "tests/data/test_data.csv"
    received = DataFrameWrapper.from_files(
        spark = spark_context,
        data_path=data_path,
        #data_path=#datapathfixture,
        name='Trial csv',
    )

    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.data.schema == expected_schema
    assert_df_equality(received.data, expected_data)