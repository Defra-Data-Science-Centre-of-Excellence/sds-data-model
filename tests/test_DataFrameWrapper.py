"""Tests for DataFrame wrapper class."""

from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import pytest
from chispa.dataframe_comparer import assert_df_equality
from dask.array import arange, concatenate, ones, zeros
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pytest import FixtureRequest, fixture, raises
from shapely.geometry import box
from xarray import DataArray, Dataset, open_dataset
from xarray.testing import assert_identical

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, BNG_YMIN, CELL_SIZE
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
            StructField("category", StringType(), True),
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
        {"category": "x", "a": 1, "b": 4, "c": 7},
        {"category": "x", "a": 2, "b": 5, "c": 8},
        {"category": "y", "a": 3, "b": 6, "c": 9},
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
    expected_dataframewrapper_name: str,
    expected_dataframe: SparkDataFrame,
    expected_empty_metadata: None,
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )

    assert received.name == expected_dataframewrapper_name
    assert received.metadata == expected_empty_metadata
    assert_df_equality(received.data, expected_dataframe)


@fixture
def expected_dataframe_limit(
    spark_session: SparkSession,
    expected_schema: StructType,
) -> SparkDataFrame:
    """Expected data when using limit call method."""
    data: Iterable = [
        {"category": "x", "a": 1, "b": 4, "c": 7},
        {"category": "x", "a": 2, "b": 5, "c": 8},
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
        {"category": "y", "a": 3, "b": 6, "c": 9},
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
        ("show", None, None, "expected_dataframe"),
    ),
    ids=(
        "limit",
        "select",
        "filter",
        "show",
    ),
)
def test_call_method(
    spark_session: SparkSession,
    temp_path: str,
    method_name: str,
    method_args: Optional[Union[str, Sequence[str]]],
    method_kwargs: Optional[Dict[str, Any]],
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
        {"category": "x", "a": 1, "b": 4, "c": 7, "d": 10, "e": 13},
        {"category": "x", "a": 2, "b": 5, "c": 8, "d": 11, "e": 14},
        {"category": "y", "a": 3, "b": 6, "c": 9, "d": 12, "e": 15},
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
    received.call_method("join", other=dataframe_other, on="a")  # type: ignore[arg-type]  # noqa: B950
    assert_df_equality(received.data, expected_dataframe_joined)


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


def test_call_method_groupBy(
    spark_session: SparkSession,
    temp_path: str,
    expected_dataframe_grouped: SparkDataFrame,
) -> None:
    """Passing the `.agg` method to `.call_method` produces the expected results."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )
    received.call_method("groupBy", "category").call_method("avg")  # type: ignore[arg-type]  # noqa: B950
    assert_df_equality(received.data, expected_dataframe_grouped)


@fixture
def hl_schema() -> StructType:
    """Schema HL DataFrame."""
    return StructType(
        [
            StructField("bng_index", StringType(), True),
            StructField("bounds", ArrayType(IntegerType()), True),
            StructField("geometry", BinaryType(), True),
        ]
    )


@fixture
def hl_dataframe(
    hl_schema: StructType,
    spark_session: SparkSession,
) -> SparkDataFrame:
    """A DataFrame containing the HL cell of the BNG."""
    bounds = (0, 1_200_000, 100_000, 1_300_000)

    data: Iterable = [
        {"bng_index": "HL", "bounds": bounds, "geometry": box(*bounds).wkb},
    ]

    df: SparkDataFrame = spark_session.createDataFrame(
        data=data,
        schema=hl_schema,
    )

    return df


@fixture
def hl_wrapper(
    hl_dataframe: SparkDataFrame,
) -> DataFrameWrapper:
    """A wrapper for the HL DataFrame."""
    return DataFrameWrapper(
        name="hl",
        data=hl_dataframe,
        metadata=None,
    )


@fixture
def hl_zarr_path(tmp_path: Path, hl_wrapper: DataFrameWrapper) -> str:
    """Where the `zarr` file will be saved."""
    path = str(tmp_path / "hl.zarr")
    hl_wrapper.to_zarr(
        path=path,
        data_array_name="hl",
    )
    return path


@fixture
def expected_hl_dataset() -> Dataset:
    """What we would expect the HL dataset to look like."""
    hl = ones(dtype="uint8", shape=(10_000, 10_000), chunks=(10_000, 10_000))
    top_row_rest = zeros(dtype="uint8", shape=(10_000, 60_000), chunks=(10_000, 10_000))
    top_row = concatenate([hl, top_row_rest], axis=1)

    rest = zeros(dtype="uint8", shape=(120_000, 70_000), chunks=(10_000, 10_000))

    expected_array = concatenate([top_row, rest], axis=0)

    coords = {
        "northings": arange(BNG_YMAX - (CELL_SIZE / 2), BNG_YMIN, -CELL_SIZE),
        "eastings": arange(BNG_XMIN + (CELL_SIZE / 2), BNG_XMAX, CELL_SIZE),
    }

    expected_data_array = DataArray(
        data=expected_array,
        coords=coords,
        name="hl",
    )

    return Dataset(
        data_vars={
            "hl": expected_data_array,
        },
    )


def test_to_zarr(
    hl_zarr_path: str,
    expected_hl_dataset: Dataset,
) -> None:
    """The HL DataFrame wrapper is rasterised as expected."""
    hl_dataset = open_dataset(
        hl_zarr_path,
        engine="zarr",
        decode_coords=True,
        mask_and_scale=False,
        chunks={
            "eastings": 10_000,
            "northings": 10_000,
        },
    )

    assert_identical(hl_dataset, expected_hl_dataset)


def test_to_zarr_with_metadata(
    hl_zarr_path_with_metadata: str,
    expected_hl_dataset_with_metadata: Dataset,
) -> None:
    """Check that attrs in the zarr look as expected."""
    hl_dataset = open_dataset(
        hl_zarr_path_with_metadata,
        engine="zarr",
        decode_coords=True,
        mask_and_scale=False,
        chunks={
            "eastings": 10_000,
            "northings": 10_000,
        },
    )

    assert hl_dataset.attrs == expected_hl_dataset_with_metadata.attrs


@pytest.mark.parametrize(
    argnames=[
        "out_path",
    ],
    argvalues=[
        ("",),
        ("hl.zarr",),
    ],
    ids=[
        "directory",
        ".zarr file",
    ],
)
def test_zarr_overwrite_check(
    out_path: str,
    tmp_path: Path,
    hl_wrapper_no_metadata: DataFrameWrapper,
) -> None:
    """Check error thrown when a zarr output path already contains a zarr."""
    with raises(ValueError, match="Zarr file already exists"):

        hl_wrapper_no_metadata.to_zarr(
            path=str(tmp_path / out_path), data_array_name="tmp_zarr"
        )

        hl_wrapper_no_metadata.to_zarr(
            path=str(tmp_path / out_path), data_array_name="tmp_zarr"
        )
