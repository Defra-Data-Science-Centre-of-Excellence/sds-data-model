"""Tests for Metadata module."""
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import pytest
from chispa.dataframe_comparer import assert_df_equality
from dask.array import concatenate, ones, zeros
from numpy import arange
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pytest import FixtureRequest, fixture
from shapely.geometry import box
from xarray import DataArray, Dataset, open_dataset
from xarray.testing import assert_identical

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, BNG_YMIN, CELL_SIZE
from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.metadata import Metadata


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
def expected_empty_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None


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
        metadata=expected_metadata,
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
def expected_attrs(metadata: Metadata) -> Dict:
    """What we would expect the metadata in attrs to look like."""
    _metadata = asdict(metadata) if metadata else None
    return _metadata


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
        coords=coords,
        attrs=expected_attrs,
    )