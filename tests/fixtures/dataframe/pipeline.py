from pathlib import Path
from typing import Callable, List, Dict, Tuple, Union

from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StringType, BinaryType, StructType, LongType, ArrayType, IntegerType
from pytest import fixture
from shapely.wkb import loads
from shapely.geometry import box

from itertools import chain, repeat

from dask import delayed
from dask.array import from_delayed
from numpy import zeros, ones
from xarray import DataArray, Dataset

from sds_data_model.constants import BoundingBox

from sds_data_model.constants import (
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    BOX_SIZE,
    CELL_SIZE,
)

# create a new fixture which has smaller boxes centred on the original boxes
# produce a list of tuples comparing bounds for 20_000 x 20_000 boxes with their
# centres on the existing 100_000 grid system. 

@fixture
def small_boxes(
    BNG_XMIN: int,
    BNG_XMAX: int,
    BNG_YMIN: int,
    BNG_YMAX: int,
    BOX_SIZE: int,
) -> List[Tuple]:
    x_centres = list(range(int(xmin+(box_size/2)), xmax, box_size))
    y_centres = list(range(int(ymin+(box_size/2)), ymax, box_size))
    comb = list(product(x_centres, y_centres))
    collector = []
    for centre in comb:
        cell_xmin = centre[0]-10_000
        cell_xmax = centre[0]+10_000
        cell_ymin = centre[1]-10_000
        cell_ymax = centre[1]+10_000
        collector.append((cell_xmin, cell_ymin, cell_xmax, cell_ymax))
    
    return collector


# turn them into geometries
@fixture
def new_geometry_column(
    small_boxes: Tuple[BoundingBox,...],
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

# create a set of data that can feed into creating a raster
@fixture
def new_data(
    new_string_category_column: Tuple[str, ...],
    new_geometry_column: Tuple[bytearray, ...],
) -> List[Dict[str, Union[str, bytearray]]]:
    """Generate data of different types."""
    list_of_dicts: List[Dict[str, Union[str, bytearray]]] = [
        {
            "index": index,
            "category": str(new_string_category_column[index]),
            "geometry": geometry,
        }
        for index, geometry in enumerate(new_geometry_column)
    ]

    return list_of_dicts

# create xarray that looks as you would expect from rasterisation
@fixture
def expected_categorical_zarr(
    BNG_XMAX,
    BNG_XMIN,
    BNG_YMAX,
    BNG_YMIN,
    CELL_SIZE,
    new_num_rows,
    new_data,
    small_boxes,
) -> Dataset:
    """Generate a zarr with blocks of values in known positions
    
    This zarr will contain squares of 3 different values, in sequential order
    to represent what rasterising a set of polygons with 3 different values
    should look like. 
    """
    
    # create empty zarr of 0s with full extent
    xlen = len(list(range(BNG_XMIN, BNG_XMAX, CELL_SIZE)))
    ylen = len(list(range(BNG_YMIN, BNG_YMAX, CELL_SIZE)))
    
    main_array = DataArray(from_delayed(zeros([ylen,xlen], dtype = 'uint8'),
                                      shape = (ylen, xlen), dtype = 'unit8'),
                         name = 'main', 
                         coords = {"eastings":list(range(BNG_XMIN,(BNG_XMAX+CELL_SIZE), CELL_SIZE)),
                                   "northings":list(range(BNG_YMIN,(BNG_YMAX+CELL_SIZE), CELL_SIZE))})
    
    # for each polygon geometry, create an xarray and update the values in 
    # main_array with the relevant values
    
    for row in range(0,new_num_rows):
        focal_box = small_boxes[row]
        sub_xlen = len(list(range(focal_box[0], focal_box[2], CELL_SIZE))) 
        sub_ylen = len(list(range(focal_box[1], focal_box[3], CELL_SIZE)))
        
        # NEED TO ADD THE CATEGORISED LOOKUP VALUE TO new_data for each row (VAL) - determined in new_data
        # Does the box size (e.g. 20000 x 20000) need to be a fixture?
        
        sub_array = DataArray(from_delayed(ones([20000,20000]*VAL, dtype = 'unint8'),
                                           shape = (sub_ylen, sub_xlen), dtype = 'unint8'),
                              name = 'sub',
                              coords = {"eastings":list(range(focal_box[0],(focal_box[2]+CELL_SIZE), CELL_SIZE)),
                                        "northings":list(range(focal_box[1],(focal_box[3]+CELL_SIZE), CELL_SIZE))})
        
        # update the main array
        main_array = sub_array.combine_first(main_array)
        
    main_array.to_dataset()
    
    
    


    
    



# at this stage we have a dataset ready to rasterise
# the rasterised output will be used to compare to what the test is creating

# create an array that looks as expected - loop through values 

# create array of 0s loop through box values and update the array
# make a dask array for starters then turn it into an xarray dataarray

# then wrap this dataarray in an xarray dataset in a datasetwrapper and compare

# use xarray data comparison in test. 

@fixture
def make_dummy_vector_file(
    tmp_path: Path,
    data: List[Dict[str, Union[str, bytearray]]],
) -> Callable[[str], None]:
    def _make_dummy_vector_file(ext: str):
        out_path = tmp_path / f"tmp_geodata{ext}"
        df = DataFrame(data)
        gdf = GeoDataFrame(df, geometry=GeoSeries.from_wkb(df.geometry))
        gdf.to_file(out_path)
    return _make_dummy_vector_file


@fixture
def make_dummy_csv(
    tmp_path: Path,
) -> str:
    output_path = tmp_path / 'dummy.csv'
    DataFrame(
    {
        "category": ["A", "B", "C", "D"],
        "land_cover":['grassland', 'woodland', 'wetland', "farmland"] 
    }
    ).to_csv(output_path)
    return str(output_path)



@fixture
def schema() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("index", LongType(), True),
            StructField("category", StringType(), True),
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
    data: List[Dict[str, Union[str, bytearray]]],
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
