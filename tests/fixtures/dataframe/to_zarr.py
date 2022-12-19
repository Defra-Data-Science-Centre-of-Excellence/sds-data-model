"""Fixture for testing DataFrameWrapper.to_zarr."""
from itertools import chain, repeat
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import rioxarray  # noqa: F401
from affine import Affine
from dask.array import Array, arange, empty, full, linspace, ones, zeros
from numpy import float32, float64, int16, int32, int64, nan, uint8
from numpy.typing import NDArray
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)
from pytest import fixture
from shapely.geometry import box
from xarray import DataArray, Dataset

from sds_data_model.constants import (
    BBOXES,
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    INT16_MAXIMUM,
    INT16_MINIMUM,
    INT32_MAXIMUM,
    INT32_MINIMUM,
    INT64_MAXIMUM,
    INT64_MINIMUM,
    BoundingBox,
    float32_maximum,
    float32_minimum,
    float64_maximum,
    float64_minimum,
)
from sds_data_model.dataframe import DataFrameWrapper


@fixture
def schema_for_rasterisation() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("bool", BooleanType(), True),
            StructField("int16", ShortType(), True),
            StructField("int32", IntegerType(), True),
            StructField("int64", LongType(), True),
            StructField("float32", FloatType(), True),
            StructField("float64", DoubleType(), True),
            StructField("string_category", StringType(), True),
            StructField("bng_index", StringType(), True),
            StructField("bounds", ArrayType(IntegerType()), True),
            StructField("geometry", BinaryType(), True),
        ]
    )


@fixture
def bounds_column(bboxes: Tuple[BoundingBox, ...] = BBOXES) -> Tuple[BoundingBox, ...]:
    """Generate bounds column from `BBOXES`."""
    return tuple(bbox for index, bbox in enumerate(bboxes) if index % 2 == 0)


@fixture
def geometry_column(
    bounds_column: Tuple[BoundingBox, ...],
) -> Tuple[bytearray, ...]:
    """Geometry column from bounds."""
    return tuple(box(*bounds).wkb for bounds in bounds_column)


@fixture
def bng_index_column() -> Tuple[str, ...]:
    """BNG 100km grid squares."""
    return (
        "HL",
        "HN",
        "HP",
        "JM",
        "HR",
        "HT",
        "JQ",
        "HV",
        "HX",
        "HZ",
        "JW",
        "NB",
        "ND",
        "OA",
        "NF",
        "NH",
        "NK",
        "OG",
        "NM",
        "NO",
        "OL",
        "NQ",
        "NS",
        "NU",
        "OR",
        "NW",
        "NY",
        "OV",
        "SA",
        "SC",
        "SE",
        "TB",
        "SG",
        "SJ",
        "TF",
        "SL",
        "SN",
        "SP",
        "TM",
        "SR",
        "ST",
        "TQ",
        "SV",
        "SX",
        "SZ",
        "TW",
    )


@fixture
def num_rows(
    bounds_column: Tuple[BoundingBox, ...],
) -> int:
    """Return length of bounds."""
    return len(bounds_column)


@fixture
def int16_column(
    num_rows: int,
    int16_minimum: int = INT16_MINIMUM,
    int16_maximum: int = INT16_MAXIMUM - 1,
) -> Array:
    """Generate int16 minmax range."""
    return linspace(
        int16_minimum,
        int16_maximum,
        num=num_rows,
        dtype=int16,
    )


@fixture
def int32_column(
    num_rows: int,
    int32_minimum: int = INT32_MINIMUM,
    int32_maximum: int = INT32_MAXIMUM - 1,
) -> Array:
    """Generate int32 minmax range."""
    return linspace(
        int32_minimum,
        int32_maximum,
        num=num_rows,
        dtype=int32,
    )


@fixture
def int64_column(
    num_rows: int,
    int64_minimum: int = INT64_MINIMUM,
    int64_maximum: int = INT64_MAXIMUM - 1,
) -> Array:
    """Generate int64 minmax range."""
    return linspace(
        int64_minimum,
        int64_maximum,
        num=num_rows,
        dtype=int64,
    )


@fixture
def float32_column(
    num_rows: int,
    float32_minimum: float = float32_minimum,
    float32_maximum: float = float32_maximum - 1,
) -> Array:
    """Generate float32 minmax range."""
    return linspace(
        float32_minimum,
        float32_maximum,
        num=num_rows,
        dtype=float32,
    )


@fixture
def float64_column(
    num_rows: int,
    float64_minimum: float = float64_minimum,
    float64_maximum: float = float64_maximum - 1,
) -> Array:
    """Generate float64 minmax range."""
    return linspace(
        float64_minimum,
        float64_maximum,
        num=num_rows,
        dtype=float64,
    )


@fixture
def boolean_column(num_rows: int) -> Tuple[bool, ...]:
    """Generate boolean sequence."""
    return tuple(chain.from_iterable(repeat([True, False], num_rows)))


@fixture
def string_category_column(num_rows: int) -> Tuple[str, ...]:
    """Generate string sequence."""
    return tuple(chain.from_iterable(repeat("ABC", num_rows)))


@fixture
def data_for_rasterisation(
    boolean_column: Tuple[bool, ...],
    int16_column: NDArray[int16],
    int32_column: NDArray[int32],
    int64_column: NDArray[int64],
    float32_column: NDArray[float32],
    float64_column: NDArray[float64],
    string_category_column: Tuple[str, ...],
    bng_index_column: Tuple[str, ...],
    bounds_column: Tuple[BoundingBox, ...],
    geometry_column: Tuple[bytearray, ...],
) -> List[Dict[str, Union[bool, int, float, str, Tuple[int, ...], bytearray]]]:
    """Generate data of different types."""
    list_of_dicts: List[
        Dict[str, Union[bool, int, float, str, Tuple[int, ...], bytearray]]
    ] = [
        {
            "bool": bool(boolean_column[index]),
            "int16": int(int16_column[index]),
            "int32": int(int32_column[index]),
            "int64": int(int64_column[index]),
            "float32": float(float32_column[index]),
            "float64": float(float64_column[index]),
            "string_category": str(string_category_column[index]),
            "bng_index": str(bng_index_column[index]),
            "bounds": tuple(bounds_column[index]),
            "geometry": geometry,
        }
        for index, geometry in enumerate(geometry_column)
    ]

    return list_of_dicts


@fixture
def dataframe_for_rasterisations(
    spark_session: SparkSession,
    data_for_rasterisation: List,
    schema_for_rasterisation: StructType,
) -> SparkDataFrame:
    """Generate SparkDataFrame consisting of columns of different types."""
    return spark_session.createDataFrame(
        data=data_for_rasterisation,
        schema=schema_for_rasterisation,
    )


def make_nodata_array(
    dtype: str,
    no_data: Union[int, Any],
) -> Array:
    """Generate an array of `no_data` values."""
    nodata = full(
        shape=45,
        fill_value=no_data,
        dtype=dtype,
        chunks=1,
    )
    return nodata


def interleave(data: Array, no_data: Array, dtype: str) -> Array:
    """Interleave `data` and `no_data` arrays."""
    interleaved = empty(
        shape=(data.size + no_data.size),
        dtype=dtype,
        chunks=1,
    )
    interleaved[0::2] = data
    interleaved[1::2] = no_data

    return interleaved


def make_data(
    column: Array,
    dtype: str,
    no_data: Union[int, Any],
) -> Array:
    """Make 2d data array for `DataArray`."""
    nodata = make_nodata_array(
        dtype=dtype,
        no_data=no_data,
    )

    interleaved = interleave(
        data=column,
        no_data=nodata,
        dtype=dtype,
    )

    return interleaved.reshape(13, 7).rechunk((1, 1))


def make_coords(
    xmin: int,
    xmax: int,
    ymin: int,
    ymax: int,
    cell_size: int,
) -> Dict[str, Array]:
    """Make coords for `DataArray`."""
    return {
        "northings": arange(ymax - (cell_size / 2), ymin, -cell_size),
        "eastings": arange(xmin + (cell_size / 2), xmax, cell_size),
    }


def make_expected_numeric_array(
    column: Array,
    dtype: str,
    cell_size: int,
    no_data: Union[int, Any],
    xmin: int = BNG_XMIN,
    xmax: int = BNG_XMAX,
    ymin: int = BNG_YMIN,
    ymax: int = BNG_YMAX,
    name: Optional[str] = None,
) -> Dataset:
    """Make expected DataArray for data type."""
    data = make_data(
        column=column,
        dtype=dtype,
        no_data=no_data,
    )
    coords = make_coords(
        xmin=xmin,
        xmax=xmax,
        ymin=ymin,
        ymax=ymax,
        cell_size=cell_size,
    )
    _name = name if name else dtype
    dataset = DataArray(
        name=_name,
        data=data,
        coords=coords,
        attrs={"nodata": no_data},
    ).to_dataset()

    transform = Affine(cell_size, 0, xmin, 0, -cell_size, ymax)
    dataset.rio.write_crs("EPSG:27700", inplace=True)
    dataset.rio.write_transform(transform, inplace=True)

    return dataset


@fixture
def out_shape() -> Tuple[int, int]:
    """Mocked OUT_SHAPE."""
    return (1, 1)


@fixture
def cell_size() -> int:
    """Mocked CELL_SIZE."""
    return 100_000


@fixture
def expected_int16_dataset(
    int16_column: Array,
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising the `int16` column."""
    return make_expected_numeric_array(
        column=int16_column,
        dtype="int16",
        cell_size=cell_size,
        no_data=INT16_MAXIMUM,
    )


@fixture
def expected_int32_dataset(
    int32_column: Array,
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising the `int32` column."""
    return make_expected_numeric_array(
        column=int32_column,
        dtype="int32",
        cell_size=cell_size,
        no_data=INT32_MAXIMUM,
    )


@fixture
def expected_int64_dataset(
    int64_column: Array,
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising the `int64` column."""
    return make_expected_numeric_array(
        column=int64_column,
        dtype="int64",
        cell_size=cell_size,
        no_data=INT64_MAXIMUM,
    )


@fixture
def expected_float32_dataset(
    float32_column: Array,
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising the `float32` column."""
    return make_expected_numeric_array(
        column=float32_column,
        dtype="float32",
        cell_size=cell_size,
        no_data=nan,
    )


@fixture
def expected_float64_dataset(
    float64_column: Array,
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising the `float64` column."""
    return make_expected_numeric_array(
        column=float64_column,
        dtype="float64",
        cell_size=cell_size,
        no_data=nan,
    )


@fixture
def expected_mask_dataset(
    cell_size: int,
) -> Dataset:
    """`Dataset` that should be created by rasterising no columns."""
    _ones = ones(dtype=uint8, shape=46, chunks=1)
    _zeros = zeros(dtype=uint8, shape=45, chunks=1)

    data = (
        interleave(data=_ones, no_data=_zeros, dtype="uint8")
        .reshape(13, 7)
        .rechunk((1, 1))
    )

    coords = make_coords(
        xmin=BNG_XMIN,
        xmax=BNG_XMAX,
        ymin=BNG_XMIN,
        ymax=BNG_YMAX,
        cell_size=cell_size,
    )

    dataset = DataArray(
        name="mask",
        data=data,
        coords=coords,
        attrs={"nodata": 0},
    ).to_dataset()

    transform = Affine(cell_size, 0, BNG_XMIN, 0, -cell_size, BNG_YMAX)
    dataset.rio.write_crs("EPSG:27700", inplace=True)
    dataset.rio.write_transform(transform, inplace=True)

    return dataset


@fixture
def make_expected_dataset_path(
    tmp_path: Path,
    dataframe_for_rasterisations: SparkDataFrame,
    cell_size: int,
    out_shape: Tuple[int, int],
) -> Callable[[str, Optional[List[str]]], str]:
    """Fixture factory for generating `Dataset` and paths."""

    def _make_expected_dataset_path(
        name: str,
        columns: Optional[List[str]],
    ) -> str:
        """Write expected `Dataset` and return path."""
        path = str(tmp_path / name)
        DataFrameWrapper(
            name=name,
            data=dataframe_for_rasterisations,
            metadata=None,
            lookup=None,
        ).to_zarr(
            path=path,
            columns=columns,
            cell_size=cell_size,
            out_shape=out_shape,
        )
        return path

    return _make_expected_dataset_path


# @fixture
# def hl_schema() -> StructType:
#     """Schema HL DataFrame."""
#     return StructType(
#         [
#             StructField("bng_index", StringType(), True),
#             StructField("bounds", ArrayType(IntegerType()), True),
#             StructField("geometry", BinaryType(), True),
#         ]
#     )


# @fixture
# def hl_dataframe(
#     hl_schema: StructType,
#     spark_session: SparkSession,
# ) -> SparkDataFrame:
#     """A DataFrame containing the HL cell of the BNG."""
#     bounds = (0, 1_200_000, 100_000, 1_300_000)

#     data: Iterable = [
#         {"bng_index": "HL", "bounds": bounds, "geometry": box(*bounds).wkb},
#     ]

#     df: SparkDataFrame = spark_session.createDataFrame(
#         data=data,
#         schema=hl_schema,
#     )

#     return df


# @fixture
# def expected_attrs() -> Dict:
#     """What we would expect the metadata in attrs to look like."""
#     expected_attrs = {
#         "title": "Ramsar (England)",
#         "dataset_language": ["eng"],
#         "abstract": 'A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',  # noqa: B950
#         "topic_category": ["environment"],
#         "keyword": ["OpenData", "NEbatch4", "Protected sites"],
#         "lineage": "All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",  # noqa: B950
#         "metadata_date": "2020-10-21",
#         "metadata_language": "eng",
#         "resource_type": "dataset",
#         "file_identifier": "c626e031-e561-4861-8219-b04cd1002806",
#         "quality_scope": ["dataset"],
#         "spatial_representation_type": ["vector"],
#     }

#     return expected_attrs


# @fixture
# def hl_wrapper_no_metadata(
#     hl_dataframe: SparkDataFrame,
# ) -> DataFrameWrapper:
#     """A wrapper for the HL DataFrame."""
#     return DataFrameWrapper(
#         name="hl",
#         data=hl_dataframe,
#         metadata=None,
#         lookup=None,
#     )


# @fixture
# def hl_wrapper_with_metadata(
#     hl_dataframe: SparkDataFrame,
#     expected_metadata: Metadata,
# ) -> DataFrameWrapper:
#     """A wrapper for the HL DataFrame."""
#     return DataFrameWrapper(
#         name="hl",
#         data=hl_dataframe,
#         metadata=expected_metadata,
#         lookup=None,
#     )


# @fixture
# def hl_zarr_path_no_metadata(
#     tmp_path: Path, hl_wrapper_no_metadata: DataFrameWrapper
# ) -> str:
#     """Where the `zarr` file will be saved."""
#     path = str(tmp_path / "hl.zarr")
#     hl_wrapper_no_metadata.to_zarr(
#         path=path,
#     )
#     return path


# @fixture
# def hl_zarr_path_with_metadata(
#     tmp_path: Path, hl_wrapper_with_metadata: DataFrameWrapper
# ) -> str:
#     """Where the `zarr` file will be saved."""
#     path = str(tmp_path / "hl.zarr")
#     hl_wrapper_with_metadata.to_zarr(
#         path=path,
#     )
#     return path


# @fixture
# def expected_hl_dataset_no_metadata() -> Dataset:
#     """What we would expect the HL dataset (no attrs) to look like."""
#     hl = ones(dtype="uint8", shape=(10_000, 10_000), chunks=(10_000, 10_000))
#     top_row_rest = zeros(
#       dtype="uint8", shape=(10_000, 60_000),
#       chunks=(10_000, 10_000),
#       )
#     top_row = concatenate([hl, top_row_rest], axis=1)
#     rest = zeros(dtype="uint8", shape=(120_000, 70_000), chunks=(10_000, 10_000))
#     expected_array = concatenate([top_row, rest], axis=0)
#     coords = {
#         "northings": arange(BNG_YMAX - (CELL_SIZE / 2), BNG_YMIN, -CELL_SIZE),
#         "eastings": arange(BNG_XMIN + (CELL_SIZE / 2), BNG_XMAX, CELL_SIZE),
#     }
#     expected_attrs = None

#     expected_data_array = DataArray(
#         data=expected_array,
#         coords=coords,
#         name="hl",
#     )

#     return Dataset(
#         data_vars={
#             "hl": expected_data_array,
#         },
#         coords=coords,
#         attrs=expected_attrs,
#     )


# @fixture
# def expected_hl_dataset_with_metadata(
#   expected_attrs: Dict,
# ) -> Dataset:
#     """What we would expect the HL dataset (with attrs) to look like."""
#     hl = ones(dtype="uint8", shape=(10_000, 10_000), chunks=(10_000, 10_000))
#     top_row_rest = zeros(dtype="uint8", shape=(10_000, 60_000), chunks=(10_000, 10_000))
#     top_row = concatenate([hl, top_row_rest], axis=1)
#     rest = zeros(dtype="uint8", shape=(120_000, 70_000), chunks=(10_000, 10_000))
#     expected_array = concatenate([top_row, rest], axis=0)
#     coords = {
#         "northings": arange(BNG_YMAX - (CELL_SIZE / 2), BNG_YMIN, -CELL_SIZE),
#         "eastings": arange(BNG_XMIN + (CELL_SIZE / 2), BNG_XMAX, CELL_SIZE),
#     }

#     expected_data_array = DataArray(
#         data=expected_array,
#         coords=coords,
#         name="hl",
#     )

#     return Dataset(
#         data_vars={
#             "hl": expected_data_array,
#         },
#         coords=coords,
#         attrs=expected_attrs,
#     )
