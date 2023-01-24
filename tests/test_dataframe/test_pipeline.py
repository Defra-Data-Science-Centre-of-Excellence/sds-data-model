"""Tests for the `DataFrameWrapper` pipeline."""
from pathlib import Path
from struct import pack
from typing import Callable, Tuple, Optional

from chispa import assert_df_equality
from shapely.wkb import loads
from shapely.geometry.polygon import orient
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType

from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.raster import DatasetWrapper
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
    spark_session: SparkSession,
    tmp_path: Path,
    make_dummy_vector_file: Callable[[str], None],
    make_dummy_csv: str,
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
        spark=spark_session,
    )

    aspatial_dfw = DataFrameWrapper.from_files(
        data_path=make_dummy_csv,
        name="aspatial",
        spark=spark_session,
    )

    zarr_path = str(tmp_path / "joined.zarr")

    (
        spatial_dfw
        .call_method("join", other=aspatial_dfw.data, on="category")
        .call_method("filter", "land_cover != 'woodland'")
        .categorize(["land_cover"])
        .index()
        .to_zarr(
            path=zarr_path,
            columns=["land_cover"],
        )
    )

    DatasetWrapper.from_files(
        data_path=zarr_path,
    )
