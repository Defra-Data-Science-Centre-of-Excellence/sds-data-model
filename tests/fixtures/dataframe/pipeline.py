from pathlib import Path
from typing import Callable, List, Dict, Tuple, Union

from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StringType, BinaryType, StructType, LongType
from pytest import fixture
from shapely.wkb import loads


@fixture
def data(
    string_category_column: Tuple[str, ...],
    geometry_column: Tuple[bytearray, ...],
) -> List[Dict[str, Union[str, bytearray]]]:
    """Generate data of different types."""
    list_of_dicts: List[Dict[str, Union[str, bytearray]]] = [
        {
            "index": index,
            "category": str(string_category_column[index]),
            "geometry": geometry,
        }
        for index, geometry in enumerate(geometry_column)
    ]

    return list_of_dicts

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
    tmp_path: Path,) -> None:
    DataFrame(
    {
        "category": ["A", "B", "C"],
        "land_cover":['grassland', 'woodland', 'wetland'] 
    }
    ).to_csv(tmp_path / 'dummy.csv')



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
