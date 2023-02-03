from pathlib import Path
from typing import Callable, List, Dict, Tuple, Union, Iterable

from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructField,
    StringType,
    BinaryType,
    StructType,
    LongType,
    ArrayType,
    IntegerType,
)
from pytest import fixture
from shapely.wkb import loads
from shapely.geometry import box

from itertools import chain, repeat, product

from dask import delayed
from dask.array import from_delayed, ones, full, arange
from xarray import DataArray, Dataset

from sds_data_model.constants import BoundingBox

from sds_data_model.constants import (
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    BOX_SIZE,
    CELL_SIZE,
    BBOXES,
)

import rioxarray

from affine import Affine
from rioxarray.rioxarray import affine_to_coords

# create a new fixture which has smaller boxes centred on the original boxes
# produce a list of tuples containing bounds for 20_000 x 20_000 boxes with their
# centres on the existing 100_000 grid system.


@fixture
def small_boxes(
    bboxes: Tuple[int, int, int, int] = BBOXES,
) -> List[Tuple[int, int, int, int]]:
    # x_centres = list(range(int(BNG_XMIN + (BOX_SIZE / 2)), BNG_XMAX, BOX_SIZE))
    # y_centres = list(range(int(BNG_YMIN + (BOX_SIZE / 2)), BNG_YMAX, BOX_SIZE))
    # comb = list(product(x_centres, y_centres))
    # collector = []
    # for centre in comb:
    #     cell_xmin = centre[0] - 10_000
    #     cell_xmax = centre[0] + 10_000
    #     cell_ymin = centre[1] - 10_000
    #     cell_ymax = centre[1] + 10_000
    #     collector.append((cell_xmin, cell_ymin, cell_xmax, cell_ymax))

    # return collector
    centers = [box(*bbox).centroid for bbox in bboxes]
    return [
        (
            int(center.x - 10_000),
            int(center.y - 10_000),
            int(center.x + 10_000),
            int(center.y + 10_000),
        )
        for center in centers
    ]


# turn them into geometries
@fixture
def new_geometry_column(
    small_boxes: Tuple[BoundingBox, ...],
) -> Tuple[bytearray, ...]:
    """Geometry column from bounds."""
    return tuple(box(*bounds).wkb for bounds in small_boxes)


# work out the number of rows in the data
@fixture
def new_num_rows(
    small_boxes: Tuple[BoundingBox, ...],
) -> int:
    """Return length of bounds."""
    return len(small_boxes)


# repeat letters ABCD for the number of rows in the dataset
@fixture
def new_string_category_column(new_num_rows: int) -> Tuple[str, ...]:
    """Generate string sequence."""
    return tuple(chain.from_iterable(repeat("ABCD", new_num_rows)))


# repeat values 0,1,2,3 for the number of the rows in the dataset
# this indicates the lookup value that would be assigned by 'categorize'
@fixture
def new_category_lookup_column(new_num_rows: int) -> Tuple[int, ...]:
    """Generate categorical lookup sequence."""
    return tuple(chain.from_iterable(repeat([0, 1, 2, 3], new_num_rows)))


# create a set of data that can feed into creating a raster
@fixture
def new_data(
    new_string_category_column: Tuple[str, ...],
    new_category_lookup_column: Tuple[int, ...],
    new_geometry_column: Tuple[bytearray, ...],
) -> List[Dict[str, Union[int, str, bytearray]]]:
    """Generate data of different types."""
    list_of_dicts: List[Dict[str, Union[int, str, bytearray]]] = [
        {
            "index": index,
            "category": str(new_string_category_column[index]),
            "lookup_val": int(new_category_lookup_column[index]),
            "geometry": geometry,
        }
        for index, geometry in enumerate(new_geometry_column)
    ]

    return list_of_dicts


# create xarray that looks as you would expect from rasterisation
@fixture
def expected_categorical_dataset(
    new_category_lookup_column: Tuple[int, ...],
    small_boxes: List[Tuple],
    BNG_XMIN: int = BNG_XMIN,
    BNG_XMAX: int = BNG_XMAX,
    BNG_YMIN: int = BNG_YMIN,
    BNG_YMAX: int = BNG_YMAX,
    CELL_SIZE: int = CELL_SIZE,
) -> Dataset:
    """Generate an xarray Dataset with blocks of integer values in known positions.

    This zarr will contain squares of 3 different values, in sequential order
    to represent what rasterising a set of polygons with 3 different values
    should look like.
    """

    dask_array = full(shape=(BNG_YMAX / CELL_SIZE, BNG_XMAX / CELL_SIZE), fill_value=255, dtype="uint8")

    for box, lookup in zip(small_boxes, new_category_lookup_column):
        if lookup != 3:
            xmin, ymin, xmax, ymax = tuple(coordinate / CELL_SIZE for coordinate in box)
            dask_array[slice(ymin, ymax), slice(xmin, xmax)] = lookup

    data_array = DataArray(
        dask_array,
        name="land_cover",
        coords={
            "northings": arange(BNG_YMAX - (CELL_SIZE / 2), BNG_YMIN, -CELL_SIZE),
            "eastings": arange(BNG_XMIN + (CELL_SIZE / 2), BNG_XMAX, CELL_SIZE),
        },
    )

    dims = ("northings", "eastings")
    height = int(BNG_YMAX / CELL_SIZE)
    width = int(BNG_XMAX / CELL_SIZE)
    transform = Affine(CELL_SIZE, 0, BNG_XMIN, 0, -CELL_SIZE, BNG_YMAX)

    main_dataset = data_array.to_dataset()

    main_dataset.update(
        affine_to_coords(
            transform,
            height=height,
            width=width,
            y_dim=dims[0],
            x_dim=dims[1],
        ),
    )

    main_dataset.rio.write_crs("EPSG:27700", inplace=True)
    main_dataset.rio.write_transform(transform, inplace=True)
    main_dataset["land_cover"] = main_dataset["land_cover"].assign_attrs(
        {
            "lookup": {"grassland": 0, "woodland": 1, "wetland": 2, "nodata": 255},
            "nodata": 255,
        }
    )

    return main_dataset


# TODO
# @fixture
# def expected_dag_source(

# ) -> str:
#     """Generate a DAG and return it's source as a string."""

# dag = make the dag

# return dag.source


# # add the DAG source code as attrs in the Dataset
# @fixture
# def expected_categorical_dataset_with_dag(
#     expected_categorical_dataset: Dataset, expected_dag_source: str
# ) -> Dataset:

#     expected_categorical_dataset.attrs = expected_dag_source

#     return expected_categorical_dataset


@fixture
def make_dummy_vector_file(
    tmp_path: Path,
    new_data: List[Dict[str, Union[str, bytearray]]],
) -> Callable[[str], None]:
    def _make_dummy_vector_file(ext: str):
        out_path = tmp_path / f"tmp_geodata{ext}"
        df = DataFrame(new_data)
        gdf = GeoDataFrame(df, geometry=GeoSeries.from_wkb(df.geometry))
        gdf.to_file(out_path)

    return _make_dummy_vector_file


@fixture
def make_dummy_csv(
    tmp_path: Path,
) -> str:
    output_path = tmp_path / "dummy.csv"
    DataFrame(
        {
            "category": ["A", "B", "C", "D"],
            "land_cover": ["grassland", "woodland", "wetland", "farmland"],
        }
    ).to_csv(output_path, index=False)
    return str(output_path)


@fixture
def schema() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("index", LongType(), True),
            StructField("category", StringType(), True),
            StructField("lookup_val", LongType(), True),
            StructField("geometry", BinaryType(), True),
        ]
    )


@fixture
def output_schema() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("index", LongType(), True),
            StructField("category", StringType(), True),
            StructField("geometry", StringType(), True),
        ]
    )


def to_wkt(pdf: DataFrame) -> DataFrame:
    gdf = GeoDataFrame(
        pdf,
        geometry=GeoSeries.from_wkb(pdf["geometry"], crs=27700),
    )
    return gdf.to_wkt()


@fixture
def spark_dataframe(
    spark_session: SparkSession,
    data: Iterable,
    schema: StructType,
    output_schema: StructType,
) -> SparkDataFrame:
    """Generate SparkDataFrame consisting of columns of different types."""
    return (
        spark_session.createDataFrame(
            data=data,
            schema=schema,
        )
        .groupBy("index")
        .applyInPandas(to_wkt, output_schema)
    )
