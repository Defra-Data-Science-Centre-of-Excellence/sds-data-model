"""Tests for the `DataFrameWrapper` pipeline."""
from pathlib import Path
from struct import pack
from typing import Callable, Tuple, Optional

from chispa import assert_df_equality
from shapely.wkb import loads
from shapely.geometry.polygon import orient
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType

from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.constants import BBOXES

from osgeo.ogr import Open, Feature, wkbNDR
import pytest


@pytest.mark.parametrize(
    argnames="ext",
    argvalues=(
        (".shp"),
        (".geojson"),
        (".gpkg"),
        (".gml"),
        (".gdb"),
    ),
    ids=(
        "ESRI Shapefile",
        "GeoJSON",
        "GeoPackage",
        "Geography Markup Language",
        "ESRI File Geodatabase",
    ),
)
def test_pipeline(
    tmp_path: Path,
    spark_dataframe: SparkDataFrame,
    make_dummy_vector_file: Callable[[str], None],
    ext: str,
    schema: StructType,
) -> None:
    """End to end test."""
    make_dummy_vector_file(ext)
    spatial_dfw = DataFrameWrapper.from_files(
        data_path=str(tmp_path),
        read_file_kwargs={
            "suffix": ext,
            "schema": schema,
            "coerce_to_schema":True,
        },
        name="spatial",
    ).call_method(
        "withColumn", "geometry", udf(lambda x: orient(loads(bytes(x))).wkt)("geometry")
    )

    aspatial_dfw = DataFrameWrapper.from_files(
        data_path=str(tmp_path),
        read_file_kwargs={
            "suffix": ".csv",
        },
        name="aspatial",
    )

    

    assert_df_equality(dfw.data, spark_dataframe)


# def _get_geometry(feature: Feature) -> Tuple[Optional[bytearray]]:
#     """Given a GDAL Feature, return the geometry field, if there is one."""
#     geometry = feature.GetGeometryRef()
#     if geometry:
#         return (geometry.ExportToIsoWkb(wkbNDR),)
#     else:
#         return (None,)


# def test_wkb(
#     tmp_path: Path,
#     make_dummy_vector_file,
# ) -> None:
#     make_dummy_vector_file(".shp")

#     dat = Open(str(tmp_path / "tmp_geodata.shp"))
#     l = dat.GetLayer()

#     f = l.GetFeature(0)
#     wkb = _get_geometry(f)
#     # expected_wkb = pack("<cIdddddddddddddddd", 1, 3, 100_000, 1_200_000, 100_000, 1_300_000, 0, 1_300_000, 0, 1_200_000, 100_000 1_200_000)
#     expected_wkb = bytearray(
#         b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80O2A\x00\x00\x00\x00\x00j\xf8@\x00\x00\x00\x00\x80O2A\x00\x00\x00\x00\x00j\xf8@\x00\x00\x00\x00\xd63A\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd63A\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80O2A'
#     )

#     assert wkb == expected_wkb
