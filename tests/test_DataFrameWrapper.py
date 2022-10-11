from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union, Type, Any, Dict
import os

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pytest import fixture
from pyspark.pandas import read_excel, DataFrame

from sds_data_model.dataframe import DataFrameWrapper

expected_data = {'a':[1,2,3],
       'b':[3,4,5]}
expected_df= DataFrame(expected_data)


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

@fixture
def create_temp_file(tmpdir):
    
    
    expected_df.to_csv("temp.csv", index = False, header = True)
    
    p = tmpdir.join("temp.csv")
    return p
    #write somethign to temp directory and return its name
    # return p (path)

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
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ])

@fixture
def expected_data(spark_context):
    expected_spark = spark_context.createDataFrame(expected_df, schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ]))
    return expected_spark

        
def test_vector_layer_from_files(
    spark_context,
    #shared_datadir: Path,
    create_temp_file,
    expected_name: str,
    expected_schema,
    expected_metadata: None,
    expected_data
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""

    received = DataFrameWrapper.from_files(
        spark = spark_context,
        data_path=create_temp_file,
        name='Trial csv',
    )

    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.data.schema == expected_schema
    assert_df_equality(received.data, expected_data)