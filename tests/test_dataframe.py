from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, when, array, lit
from pyspark.sql.types import StructType, StructField, StringType
from pytest import fixture
from pandas import DataFrame
from geopandas import GeoDataFrame
from shapely.geometry import box
from dask.array import ones, concatenate, zeros
from xarray import open_dataset
from numpy import array_equal

from sds_data_model.dataframe import DataFrameWrapper

geometry = box(0, 1_200_000, 100_000, 1_300_000)

data = {
    "bng": ["HL"],
    "geometry": [geometry]}

expected_gdf = GeoDataFrame(
    data=data,
    crs="EPSG:27700",
    )

bounds = [0, 1200000, 100000, 1300000]

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

@fixture(scope="session")
def temp_shapefile(tmpdir_factory):
    """Create a temporary directory and data for testing.

    Args:
        tmpdir_factory (Instance): temporary directory instance

    Returns:
        object: object containing path to temporary data
    """
    file_path = tmpdir_factory.mktemp("data").join("temp_shapefile.shp")
    expected_gdf.to_file(str(file_path))
    return file_path

@fixture
def test_shapefile(
    spark_session: SparkSession,
    temp_shapefile
    ):
    """Creating a DataFrameWrapper object from the shapefile"""

    test_shapefile = DataFrameWrapper.from_files(
        spark = spark_session,
        name = "temp_shapefile",
        data_path = str(temp_shapefile)[:-18],
        read_file_kwargs = {'suffix':'.shp', 'ideal_chunk_size':1000})

    return test_shapefile

@fixture
def temp_zarr_file(tmpdir_factory, test_shapefile):
    """Writing the DataFrameWrapper object to zarr file

    Args:
        tmpdir_factory (Instance): temporary directory instance

    Returns:
        object: object containing path to temporary data
    """

    shapefile = test_shapefile.call_method("withColumn", "bounds", when(col('bng') == 'HL', array([lit(number) for number in bounds])))

    zarr_path = tmpdir_factory.mktemp("data").join("temp_zarr_file.zarr")
    shapefile.to_zarr(path = str(zarr_path), data_array_name = "test_zarr")
    return zarr_path

@fixture
def expected_data_chunk():
    """Expected Data Array chunk"""

    hl = ones(dtype="uint8", shape=(10_000, 10_000))
    top_row_rest = zeros(dtype="uint8", shape=(10_000, 10_000)).repeat(6, axis=1)
    top_row = concatenate([hl, top_row_rest], axis=1)
    rest = zeros(dtype="uint8", shape=(120_000, 70_000))
    expected_array = concatenate([top_row, rest], axis=0)

    expected_chunk = expected_array[:10_000,:10_000]

    return expected_chunk


def test_to_zarr(
    temp_zarr_file,
    expected_data_chunk
) -> None:
    """Function to test the returned zarr file.

    Args:
        temp_zarr_file (SparkDataFrame): temporary file path for test data
        expected_data_chunk (DaskArray): expected data output for the zarr file
    """
    received = open_dataset(
        temp_zarr_file,
        chunks={"northings": 10_000, "eastings": 10_000}, 
        engine="zarr",
    )

    zarr_chunk = received["test_zarr"][:10_000,:10_000].data
    
    assert array_equal(zarr_chunk.compute(), expected_data_chunk.compute())