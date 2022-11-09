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
from tests.fixtures.fixtures_metadata import expected_metadata


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
def expected_dataframe_schema() -> StructType:
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
    expected_dataframe_schema: StructType,
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
        {"a": 1, "b": 4, "c": 7},
        {"a": 2, "b": 5, "c": 8},
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
        {"a": 3, "b": 6, "c": 9},
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
def expected_empty_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None


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
def expected_attrs() -> Dict:
    """What we would expect the metadata in attrs to look like."""
    
    expected_attrs = {
            "title": "Ramsar (England)",
            "dataset_language": ("eng",),
            "abstract": 'A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',
            "topic_category": ("environment",),
            "keyword": ("OpenData", "NEbatch4", "Protected sites"),
            "lineage": "All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",
            "metadata_date": "2020-10-21",
            "metadata_language": "eng",
            "resource_type": "dataset",
            "file_identifier": "c626e031-e561-4861-8219-b04cd1002806",
            "quality_scope": ("dataset",),
            "spatial_representation_type": ("vector",)
            }
    
    return expected_attrs
    




@fixture
def hl_wrapper_no_metadata(
    hl_dataframe: SparkDataFrame,
) -> DataFrameWrapper:
    """A wrapper for the HL DataFrame."""
    return DataFrameWrapper(
        name="hl",
        data=hl_dataframe,
        metadata=None,
    )


@fixture
def hl_wrapper_with_metadata(
    hl_dataframe: SparkDataFrame,
    expected_metadata: Metadata,
) -> DataFrameWrapper:
    """A wrapper for the HL DataFrame."""
    return DataFrameWrapper(
        name="hl",
        data=hl_dataframe,
        metadata=expected_metadata,
    )


@fixture
def hl_zarr_path_no_metadata(
    tmp_path: Path, hl_wrapper_no_metadata: DataFrameWrapper
) -> str:
    """Where the `zarr` file will be saved."""
    path = str(tmp_path / "hl.zarr")
    hl_wrapper_no_metadata.to_zarr(
        path=path,
        data_array_name="hl",
    )
    return path


@fixture
def hl_zarr_path_with_metadata(
    tmp_path: Path, hl_wrapper_with_metadata: DataFrameWrapper
) -> str:
    """Where the `zarr` file will be saved."""
    path = str(tmp_path / "hl.zarr")
    hl_wrapper_with_metadata.to_zarr(
        path=path,
        data_array_name="hl",
    )
    return path


@fixture
def expected_hl_dataset_no_metadata() -> Dataset:
    """What we would expect the HL dataset (no attrs) to look like."""
    hl = ones(dtype="uint8", shape=(10_000, 10_000), chunks=(10_000, 10_000))
    top_row_rest = zeros(dtype="uint8", shape=(10_000, 60_000), chunks=(10_000, 10_000))
    top_row = concatenate([hl, top_row_rest], axis=1)
    rest = zeros(dtype="uint8", shape=(120_000, 70_000), chunks=(10_000, 10_000))
    expected_array = concatenate([top_row, rest], axis=0)
    coords = {
        "northings": arange(BNG_YMAX - (CELL_SIZE / 2), BNG_YMIN, -CELL_SIZE),
        "eastings": arange(BNG_XMIN + (CELL_SIZE / 2), BNG_XMAX, CELL_SIZE),
    }
    expected_attrs = None

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


@fixture
def expected_hl_dataset_with_metadata(
    expected_attrs: Dict) -> Dataset:
    """What we would expect the HL dataset (with attrs) to look like."""
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
