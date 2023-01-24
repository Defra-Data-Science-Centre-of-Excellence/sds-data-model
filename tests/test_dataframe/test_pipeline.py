"""Tests for the `DataFrameWrapper` pipeline."""
from pathlib import Path
from struct import pack
from typing import Callable

from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame

from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.constants import BBOXES

from osgeo.ogr import Open, Feature, WkbNDR, DataSource
import pytest
from pyspark_vector_files import _get_layer_names, _get_features


@pytest.mark.parametrize(
    argnames=("ext"),
    arvalues=(
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
    )
)
def test_pipeline(
    tmp_path: Path,
    spark_dataframe: SparkDataFrame,
    make_dummy_vector_file: Callable[[str], None],
    ext: str,
) -> None:
    """End to end test."""
    make_dummy_vector_file(ext)
    dfw = DataFrameWrapper.from_files(
        data_path=str(tmp_path),
        read_file_kwargs={
            "suffix": ext,
        },
        name="test",

    )

    assert_df_equality(dfw.data, spark_dataframe)


def _get_geometry(feature: Feature) -> Tuple[Optional[bytearray]]:
    """Given a GDAL Feature, return the geometry field, if there is one."""
    geometry = feature.GetGeometryRef()
    if geometry:
        return (geometry.ExportToWkb(wkbNDR),)
    else:
        return (None,)


def test_wkb(
    tmp_path: Path,
    make_dummy_vector_file,
) -> None:
    make_dummy_vector_file(".shp")

    dat = Open(tmp_path / "tmp_geodata.shp")
    l = dat.GetLayer()
    
    f = l.GetFeature(0)
    wkb = _get_geometry(f)
    expected_wkb = pack("<cIdddddddddddddddd", 1, 3, *BBOXES[0])

    assert wkb == expected_wkb
