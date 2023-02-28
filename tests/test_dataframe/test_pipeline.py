"""Tests for the `DataFrameWrapper` pipeline."""
from itertools import product
import logging

from pathlib import Path
from struct import pack
from sys import stdout
from typing import Callable, Tuple, Optional

from dask.array import all, equal
from numpy import unique
from chispa import assert_df_equality
from shapely.wkb import loads
from shapely.geometry.polygon import orient
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType

from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.raster import DatasetWrapper
from sds_data_model.constants import BBOXES

from xarray import Dataset, open_dataset
from xarray.testing import assert_identical

from osgeo.ogr import Open, Feature, wkbNDR
import pytest


# The aim of this test is to test the whole pipeline of the datamodel including
# reading from various sources, executing some methods on the dataframes (including join)
# categorizing the data, indexing and writing to zarr. Ultimately the zarr file
# and associated DAG will be checked for comparison.

# The 4 classes in the csv are joined, filtered down to 3 and then categorized.
# Having 3 classes will test that categorization works as the results should be 3
# unique values, rather than a binary mask of 2.

# In the test, the last categorical value is filtered out of the data, so that
# when categorized, the values match those in the fixture (no re-ordering).

# A/B/C/D are used as adjoining values as that's what the dummy vector data inherits
# from other fixtures.

# 1) A series of dummy vector files in different formats are created
# 2) A dummy csv is created
# 3) Both are read in as dataframewrappers.
# 4) They are joined, filtered and categorized.
# 5) Then indexed
# 6) Then written as a zarr file, using the categorized column.

# Possibly add metadata into the mix...


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
    expected_categorical_dataset: Dataset,
) -> None:
    """End to end test."""
    make_dummy_vector_file(ext)

    spatial_dfw = DataFrameWrapper.from_files(
        data_path=str(tmp_path),
        read_file_kwargs={
            "suffix": ext,
            "schema": schema,
            "coerce_to_schema": True,
        },
        name="spatial",
        spark=spark_session,
    )

    # Debug
    # spatial_dfw.data.show()

    aspatial_dfw = DataFrameWrapper.from_files(
        data_path=make_dummy_csv,
        name="aspatial",
        spark=spark_session,
    )

    # Debug
    # aspatial_dfw.data.show()

    zarr_path = str(tmp_path / "joined.zarr")

    joined_dfw = (
        spatial_dfw.call_method("join", other=aspatial_dfw.data, on="category")
        .call_method("filter", "land_cover != 'farmland'")
        .categorize(["land_cover"])
        .index()
    )

    # Debug
    # joined_dfw.data.show()

    joined_dfw.to_zarr(
        path=zarr_path,
        columns=["land_cover"],
    )
    out_data = open_dataset(
        zarr_path,
        engine="zarr",
        chunks={"northings": 10_000, "eastings": 10_000},
    )
    print("TEST OUTPUT")
    print(out_data.keys())
    print(out_data.attrs)
    print("FIXTURE OUTPUT")
    print(expected_categorical_dataset.keys())
    print(expected_categorical_dataset.attrs)

    # expected_categorical_dataset doesn't have dag
    del out_data.attrs["DAG_source"]
    assert_identical(out_data, expected_categorical_dataset)

    # Debug
    # for northing, easting in product(
    #    range(0, 130_000, 10_000), range(0, 70_000, 10_000)
    # ):
    #    northing_index = slice(northing, northing + 10_000)
    #    easting_index = slice(easting, easting + 10_000)
    #    got = out_data["land_cover"][northing_index, easting_index].compute()
    #    expected = expected_categorical_dataset["land_cover"][
    #        northing_index, easting_index
    #    ].compute()


#
#    print(f"Sub array [{northing_index}, {easting_index}]")
#
#    got_values, got_counts = unique(got, return_counts=True)
#    expected_values, expected_counts = unique(expected, return_counts=True)
#
#    print(f"GOT has:")
#    for value, count in zip(got_values, got_counts):
#        print(f"\tVALUE: {value}, COUNT: {count}")
#    print(f"EXPECTED has:")
#    for value, count in zip(expected_values, expected_counts):
#        print(f"\tVALUE: {value}, COUNT: {count}")
#    print("")
