from pathlib import Path
from typing import List, Dict, Tuple, Union

from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructField, StringType, BinaryType, StructType
from pytest import fixture


@fixture
def data(
    string_category_column: Tuple[str, ...],
    geometry_column: Tuple[bytearray, ...],
) -> List[Dict[str, Union[str, bytearray]]]:
    """Generate data of different types."""
    list_of_dicts: List[Dict[str, Union[str, bytearray]]] = [
        {
            "category": str(string_category_column[index]),
            "geometry": geometry,
        }
        for index, geometry in enumerate(geometry_column)
    ]

    return list_of_dicts


@fixture(autouse=True)
def make_dummy_shp(
    tmp_path: Path,
    data: List[Dict[str, Union[str, bytearray]]],
) -> None:

    out_path = tmp_path / "tmp_geodata.shp"

    df = DataFrame(data)

    gdf = GeoDataFrame(df, geometry=GeoSeries.from_wkb(df.geometry))
    gdf.to_file(out_path)


@fixture
def schema() -> StructType:
    """Schema for different data types."""
    return StructType(
        fields=[
            StructField("category", StringType(), True),
            StructField("geometry", BinaryType(), True),
        ]
    )


@fixture
def spark_dataframe(
    spark_session: SparkSession,
    data: List[Dict[str, Union[str, bytearray]]],
    schema: StructType,
) -> SparkDataFrame:
    """Generate SparkDataFrame consisting of columns of different types."""
    return spark_session.createDataFrame(
        data=data,
        schema=schema,
    )
