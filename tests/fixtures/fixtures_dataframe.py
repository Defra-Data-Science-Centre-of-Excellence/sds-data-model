"""Fixtures for Metadata tests."""

from itertools import chain, repeat
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Union

from affine import Affine
from dask.array import concatenate, ones, zeros
from more_itertools import chunked, interleave_longest
from numpy import arange, float32, float64, int8, int16, int32, int64, linspace, uint8
from numpy.typing import NDArray
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
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
    CELL_SIZE,
    BoundingBox,
)
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
def schema_for_rasterisation() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("bool", BooleanType(), True),
            StructField("int8", ByteType(), True),
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


INT8_MINIMUM = 2**8 // -2
INT8_MAXIMUM = (2**8 // 2) - 1
INT16_MINIMUM = 2**16 // -2
INT16_MAXIMUM = (2**16 // 2) - 1
INT32_MINIMUM = 2**32 // -2
INT32_MAXIMUM = (2**32 // 2) - 1
INT64_MINIMUM = 2**64 // -2
INT64_MAXIMUM = (2**64 // 2) - 1


@fixture
def int8_column(
    num_rows: int,
    int8_minimum: int = INT8_MINIMUM,
    int8_maximum: int = INT8_MAXIMUM,
) -> NDArray[int8]:
    """Generate int8 minmax range."""
    return linspace(
        int8_minimum,
        int8_maximum,
        num=num_rows,
        dtype=int8,
    )


@fixture
def int16_column(
    num_rows: int,
    int16_minimum: int = INT16_MINIMUM,
    int16_maximum: int = INT16_MAXIMUM,
) -> NDArray[int16]:
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
    int32_maximum: int = INT32_MAXIMUM,
) -> NDArray[int32]:
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
    int64_maximum: int = INT64_MAXIMUM,
) -> NDArray[int64]:
    """Generate int64 minmax range."""
    return linspace(
        int64_minimum,
        int64_maximum,
        num=num_rows,
        dtype=int64,
    )


def _get_float_maximum(exponent: int, mantissa: int) -> float:
    return float(
        2 ** (2 ** (exponent - 1) - 1) * (1 + (2**mantissa - 1) / 2**mantissa)
    )


def _get_float_minimum(exponent: int) -> float:
    return float(2 ** (2 - 2 ** (exponent - 1)))


FLOAT32_EXPONENT = 8
FLOAT32_MANTISSA = 23
FLOAT64_EXPONENT = 11
FLOAT64_MANTISSA = 52

float32_minimum = _get_float_minimum(FLOAT32_EXPONENT)
float32_maximum = _get_float_maximum(FLOAT32_EXPONENT, FLOAT32_MANTISSA)
float64_minimum = _get_float_minimum(FLOAT64_EXPONENT)
float64_maximum = _get_float_maximum(FLOAT64_EXPONENT, FLOAT64_MANTISSA)


@fixture
def float32_column(
    num_rows: int,
    float32_minimum: float = float32_minimum,
    float32_maximum: float = float32_maximum,
) -> NDArray[float32]:
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
    float64_maximum: float = float64_maximum,
) -> NDArray[float64]:
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
    int8_column: NDArray[int8],
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
            "int8": int(int8_column[index]),
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


@fixture
def geometry_mask_path(
    tmp_path: Path,
    dataframe_for_rasterisations: SparkDataFrame,
) -> str:
    """Write geometry mask and return path."""
    path = str(tmp_path / "mask.zarr")
    DataFrameWrapper(
        name="geometry_mask",
        data=dataframe_for_rasterisations,
        metadata=None,
        lookup=None,
    ).to_zarr(
        path=path,
        cell_size=100_000,
        out_shape=(1, 1),
    )
    return path


@fixture
def out_shape() -> Tuple[int, int]:
    """Mocked OUT_SHAPE."""
    return (1, 1)


@fixture
def cell_size() -> int:
    """Mocked CELL_SIZE."""
    return 100_000


@fixture
def expected_geometry_mask(
    out_shape: Tuple[int, int],
    cell_size: int,
) -> Dataset:
    """Expected geometry mask `Dataset`."""
    _ones = ones(dtype=uint8, shape=out_shape, chunks=out_shape)
    _zeros = zeros(dtype=uint8, shape=out_shape, chunks=out_shape)

    interleaved = interleave_longest(
        repeat(_ones, 46),
        repeat(_zeros, 45),
    )

    data = concatenate(
        (concatenate(chunk, axis=1) for chunk in chunked(interleaved, 7)), axis=0
    )

    coords = {
        "northings": arange(BNG_YMAX - (cell_size / 2), BNG_YMIN, -cell_size),
        "eastings": arange(BNG_XMIN + (cell_size / 2), BNG_XMAX, cell_size),
    }

    ds = DataArray(
        name="geometry_mask",
        data=data,
        coords=coords,
        attrs={"nodata": 0},
    ).to_dataset()

    transform = Affine(cell_size, 0, BNG_XMIN, 0, -cell_size, BNG_YMAX)
    ds.rio.write_crs("EPSG:27700", inplace=True)
    ds.rio.write_transform(transform, inplace=True)

    return ds


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
        "dataset_language": ["eng"],
        "abstract": 'A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',  # noqa: B950
        "topic_category": ["environment"],
        "keyword": ["OpenData", "NEbatch4", "Protected sites"],
        "lineage": "All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",  # noqa: B950
        "metadata_date": "2020-10-21",
        "metadata_language": "eng",
        "resource_type": "dataset",
        "file_identifier": "c626e031-e561-4861-8219-b04cd1002806",
        "quality_scope": ["dataset"],
        "spatial_representation_type": ["vector"],
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
        lookup=None,
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
        lookup=None,
    )


@fixture
def hl_zarr_path_no_metadata(
    tmp_path: Path, hl_wrapper_no_metadata: DataFrameWrapper
) -> str:
    """Where the `zarr` file will be saved."""
    path = str(tmp_path / "hl.zarr")
    hl_wrapper_no_metadata.to_zarr(
        path=path,
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
def expected_hl_dataset_with_metadata(expected_attrs: Dict) -> Dataset:
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
