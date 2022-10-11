from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union, Type, Any, Dict
import os

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pytest import fixture
from pandas import read_excel, DataFrame

from sds_data_model.dataframe import DataFrameWrapper


expected_data = {'a':[1,2,3],
       'b':[3,4,5]}
expected_df= DataFrame(expected_data)

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

@fixture(scope = 'session')
def create_temp_file(tmpdir_factory):
    
    p = tmpdir_factory.mktemp("data").join("temp.csv")
    expected_df.to_csv(str(p), index = False, header = True)
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

# @fixture
# def expected_data(spark_session, create_temp_file):
#     # expected_dict = {'a':[1,2,3],
#     #    'b':[3,4,5]}
#     #create dataframe directly using spark
#     #expected_df = DataFrame(expected_dict)
#     expected_spark = spark_session.createDataFrame(str(), 
#         schema = StructType([
#             StructField('a', StringType(), True),
#             StructField('b', StringType(), True)
#         ]))
#         #create schema using string
#     return expected_spark

        
# def test_data(spark_session, expected_name, create_temp_file):
    
#     expected_spark = spark_session.createDataFrame(expected_df, 
#         schema = StructType([
#             StructField('a', StringType(), True),
#             StructField('b', StringType(), True)
#         ]))

#     other_data = spark_session.read.csv(str(create_temp_file), header = True)
#     assert 'Trial csv' == expected_name
#     assert_df_equality(other_data, expected_spark)


def test_vector_layer_from_files(
    spark_session,
    create_temp_file,
    expected_name: str,
    expected_schema,
    expected_metadata: None
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""

    expected_spark = spark_session.createDataFrame(expected_df, 
        schema = StructType([
            StructField('a', StringType(), True),
            StructField('b', StringType(), True)
        ]))

    received = DataFrameWrapper.from_files(
        spark = spark_session,
        data_path=str(create_temp_file),
         read_file_kwargs = {'header':True},
        name='Trial csv',
    )

    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.data.schema == expected_schema
    assert_df_equality(received.data, expected_spark)