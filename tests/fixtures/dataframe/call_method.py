"""Fixture for testing DataFrameWrapper.call_method."""
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pytest import fixture


@fixture
def expected_dataframe_schema() -> StructType:
    """Schema for expected DataFrame."""
    return StructType(
        [
            StructField("category", StringType(), True),
            StructField("a", IntegerType(), True),
            StructField("b", IntegerType(), True),
            StructField("c", IntegerType(), True),
        ]
    )


@fixture
def expected_dataframe(
    spark_session: SparkSession,
    expected_dataframe_schema: StructType,
) -> SparkDataFrame:
    """A dummy `DataFrame` for testing."""
    # Annotating `data` with `Iterable` stop `mypy` from complaining that
    # `Value of type variable "RowLike" of "createDataFrame" of "SparkSession" cannot
    # be "Dict[str, int]"`
    data: Iterable = [
        {"category": "x", "a": 1, "b": 4, "c": 7},
        {"category": "x", "a": 2, "b": 5, "c": 8},
        {"category": "y", "a": 3, "b": 6, "c": 9},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_dataframe_schema,
    )


@fixture
def temp_path(
    tmp_path: Path,
    expected_dataframe: SparkDataFrame,
) -> str:
    """Create a temporary directory and data for testing."""
    path = str(tmp_path / "test.csv")
    expected_dataframe.write.csv(
        path=path,
        header=True,
    )
    return path


@fixture
def expected_dataframewrapper_name() -> str:
    """Expected DataFrameWrapper name."""
    return "Trial csv"


@fixture
def expected_dataframe_limit(
    spark_session: SparkSession,
    expected_dataframe_schema: StructType,
) -> SparkDataFrame:
    """Expected data when using limit call method."""
    data: Iterable = [
        {"category": "x", "a": 1, "b": 4, "c": 7},
        {"category": "x", "a": 2, "b": 5, "c": 8},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_dataframe_schema,
    )


@fixture
def expected_schema_select() -> StructType:
    """Expected schema after columns "a" and "b" have been selected."""
    return StructType(
        [
            StructField("a", IntegerType(), True),
            StructField("b", IntegerType(), True),
        ]
    )


@fixture
def expected_dataframe_select(
    spark_session: SparkSession,
    expected_schema_select: StructType,
) -> SparkDataFrame:
    """Expected data when using select call method."""
    data: Iterable = [
        {"a": 1, "b": 4},
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema_select,
    )


@fixture
def expected_dataframe_filter(
    spark_session: SparkSession,
    expected_dataframe_schema: StructType,
) -> SparkDataFrame:
    """Expected data when using filter call method."""
    data: Iterable = [
        {"category": "y", "a": 3, "b": 6, "c": 9},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_dataframe_schema,
    )


@fixture
def schema_other() -> StructType:
    """Schema of `other` DataFrame for joining."""
    return StructType(
        [
            StructField("a", IntegerType(), True),
            StructField("d", IntegerType(), True),
            StructField("e", IntegerType(), True),
        ]
    )


@fixture
def dataframe_other(
    spark_session: SparkSession,
    schema_other: StructType,
) -> SparkDataFrame:
    """`other` DataFrame for joining."""
    data: Iterable = [
        {"a": 1, "d": 10, "e": 13},
        {"a": 2, "d": 11, "e": 14},
        {"a": 3, "d": 12, "e": 15},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=schema_other,
    )


@fixture
def expected_schema_joined() -> StructType:
    """Expected schema once `received` and `other` have been joined."""
    return StructType(
        [
            StructField("a", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("b", IntegerType(), True),
            StructField("c", IntegerType(), True),
            StructField("d", IntegerType(), True),
            StructField("e", IntegerType(), True),
        ]
    )


@fixture
def expected_dataframe_joined(
    spark_session: SparkSession,
    expected_schema_joined: StructType,
) -> SparkDataFrame:
    """Expected DataFrame once `received` and `other` have been joined."""
    data: Iterable = [
        {"a": 1, "category": "x", "b": 4, "c": 7, "d": 10, "e": 13},
        {"a": 2, "category": "x", "b": 5, "c": 8, "d": 11, "e": 14},
        {"a": 3, "category": "y", "b": 6, "c": 9, "d": 12, "e": 15},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema_joined,
    )


@fixture
def expected_schema_grouped() -> StructType:
    """Schema for expected DataFrame."""
    return StructType(
        [
            StructField("category", StringType(), True),
            StructField("avg(a)", DoubleType(), True),
            StructField("avg(b)", DoubleType(), True),
            StructField("avg(c)", DoubleType(), True),
        ]
    )


@fixture
def expected_dataframe_grouped(
    spark_session: SparkSession,
    expected_schema_grouped: StructType,
) -> SparkDataFrame:
    """A dummy `DataFrame` for testing."""
    # Annotating `data` with `Iterable` stop `mypy` from complaining that
    # `Value of type variable "RowLike" of "createDataFrame" of "SparkSession" cannot
    # be "Dict[str, int]"`
    data: Iterable = [
        {"category": "x", "avg(a)": 1.5, "avg(b)": 4.5, "avg(c)": 7.5},
        {"category": "y", "avg(a)": 3.0, "avg(b)": 6.0, "avg(c)": 9.0},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema_grouped,
    )
