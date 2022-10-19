from pathlib import Path
from chispa.dataframe_comparer import assert_df_equality
from pandas import DataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType
from pytest import fixture
import pytest
from typing import Iterable, Dict, Optional, Union, List

from sds_data_model.dataframe import DataFrameWrapper


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
def expected_schema() -> StructType:
    """Schema for expected DataFrame."""
    return StructType(
        [
            StructField("a", IntegerType(), True),
            StructField("b", IntegerType(), True),
            StructField("c", IntegerType(), True),
        ]
    )


@fixture
def expected_dataframe(
    spark_session: SparkSession,
    expected_schema: StructType,
) -> SparkDataFrame:
    """A dummy `DataFrame` for testing."""
    # Annotating `data` with `Iterable` stop `mypy` from complaining that
    # `Value of type variable "RowLike" of "createDataFrame" of "SparkSession" cannot
    # be "Dict[str, int]"`
    data: Iterable = [
        {"a": 1, "b": 4, "c": 7},
        {"a": 2, "b": 5, "c": 8},
        {"a": 3, "b": 6, "c": 9},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema,
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
def expected_name() -> str:
    """Expected DataFrameWrapper name."""
    return "Trial csv"


@fixture
def expected_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None


def test_vector_layer_from_files(
    spark_session: SparkSession,
    temp_path: str,
    expected_name: str,
    expected_dataframe: SparkDataFrame,
    expected_metadata: None,
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )

    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert_df_equality(received.data, expected_dataframe)


@fixture
def expected_dataframe_limit(
    spark_session: SparkSession,
    expected_schema: StructType,
) -> SparkDataFrame:
    """Expected data when using limit call method."""
    data: Iterable = [
        {"a": 1, "b": 4, "c": 7},
        {"a": 2, "b": 5, "c": 8},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema,
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
    expected_schema: StructType,
) -> SparkDataFrame:
    """Expected data when using filter call method."""
    data: Iterable = [
        {"a": 3, "b": 6, "c": 9},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema,
    )


@pytest.mark.parametrize(
    argnames=(
        "method_name",
        "method_args",
        "method_kwargs",
        "expected_dataframe_name",
    ),
    argvalues=(
        ("limit", None, {"num": 2}, "expected_dataframe_limit"),
        ("select", ["a", "b"], None, "expected_dataframe_select"),
        ("filter", "a = 3", None, "expected_dataframe_filter"),
    ),
    ids=(
        "limit",
        "select",
        "filter",
    ),
)
def test_call_method(
    spark_session: SparkSession,
    temp_path: str,
    method_name: str,
    method_args: Optional[Union[List[int], int]],
    method_kwargs: Optional[Dict[str, int]],
    expected_dataframe_name: str,
    request: FixtureRequest,
) -> None:
    """Function to test most common methods used by call_method."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )

    if method_args and method_kwargs:
        # If we get both, use both, unpacking the `method_kwargs` dictionary.
        received.call_method(method_name, method_args, **method_kwargs)
    elif method_args and isinstance(method_args, list):
        # If we get only method_args, and it's a `list`, use it.
        received.call_method(method_name, *method_args)
    elif method_args:
        # Now method_args has to be a single item, use it.
        received.call_method(method_name, method_args)
    elif method_kwargs:
        # And `method_kwargs` has to be a dictionary, so unpack it.
        received.call_method(method_name, **method_kwargs)
    else:
        # Do nothing
        pass

    expected_dataframe = request.getfixturevalue(expected_dataframe_name)
    assert_df_equality(received.data, expected_dataframe)


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
        {"a": 1, "b": 4, "c": 7, "d": 10, "e": 13},
        {"a": 2, "b": 5, "c": 8, "d": 11, "e": 14},
        {"a": 3, "b": 6, "c": 9, "d": 12, "e": 15},
    ]
    return spark_session.createDataFrame(
        data=data,
        schema=expected_schema_joined,
    )


def test_call_method_join(
    spark_session: SparkSession,
    temp_path: str,
    dataframe_other: SparkDataFrame,
    expected_dataframe_joined: SparkDataFrame,
) -> None:
    """Passing the `.join` method to `.call_method` produces the expected results."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )
    received.call_method("join", other=dataframe_other, on="a")
    assert_df_equality(received.data, expected_dataframe_joined)
