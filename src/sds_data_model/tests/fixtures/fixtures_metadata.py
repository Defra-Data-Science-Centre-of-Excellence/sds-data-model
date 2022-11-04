"""Tests for Metadata module."""

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import pytest
from chispa.dataframe_comparer import assert_df_equality
from dask.array import concatenate, ones, zeros
from numpy import arange
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pytest import FixtureRequest, fixture
from shapely.geometry import box
from xarray import DataArray, Dataset, open_dataset
from xarray.testing import assert_identical

from sds_data_model.constants import BNG_XMAX, BNG_XMIN, BNG_YMAX, BNG_YMIN, CELL_SIZE
from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.metadata import Metadata


@fixture
def expected_metadata() -> Metadata:
    """Expected metadata for Ramsar dataset."""
    return Metadata(
        title="Ramsar (England)",
        dataset_language=("eng",),
        abstract='A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',  # noqa: B950
        topic_category=("environment",),
        keyword=(
            "OpenData",
            "NEbatch4",
            "Protected sites",
        ),
        lineage="All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",  # noqa: B950
        metadata_date="2020-10-21",
        metadata_language="eng",
        resource_type="dataset",
        file_identifier="c626e031-e561-4861-8219-b04cd1002806",
        quality_scope=("dataset",),
        spatial_representation_type=("vector",),
    )


@fixture
def remote_url() -> str:
    """Ramsar metadata URL."""
    return "https://ckan.publishing.service.gov.uk/harvest/object/715bc6a9-1008-4061-8783-d12e9e7f38a9"  # noqa: B950 - URL


@fixture
def local_file_path(datadir: Path) -> str:
    """Ramsar metadata local file path."""
    metadata_path = datadir / "ramsar.xml"
    return str(metadata_path)


@pytest.mark.parametrize(
    argnames=[
        "path",
    ],
    argvalues=[
        ("remote_url",),
        ("local_file_path",),
    ],
    ids=[
        "Remote url",
        "Local file path",
    ],
)
@fixture
def expected_without_title() -> Metadata:
    """Expected metadata (minus title) for Ramsar dataset."""
    return Metadata(
        title=None,
        dataset_language=("eng",),
        abstract='A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',  # noqa: B950
        topic_category=("environment",),
        keyword=(
            "OpenData",
            "NEbatch4",
            "Protected sites",
        ),
        lineage="All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",  # noqa: B950
        metadata_date="2020-10-21",
        metadata_language="eng",
        resource_type="dataset",
        file_identifier="c626e031-e561-4861-8219-b04cd1002806",
        quality_scope=("dataset",),
        spatial_representation_type=("vector",),
    )


@fixture
def expected_without_keywords() -> Metadata:
    """Expected metadata (minus keywords) for Ramsar dataset."""
    return Metadata(
        title="Ramsar (England)",
        dataset_language=("eng",),
        abstract='A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites. Boundaries are mapped against Ordnance Survey MasterMap. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year]. Attribution statement: © Natural England copyright. Contains Ordnance Survey data © Crown copyright and database right [year].',  # noqa: B950
        topic_category=("environment",),
        keyword=None,
        lineage="All data is captured to the Ordnance Survey National Grid sometimes called the British National Grid. OS MasterMap Topographic Layer ? produced and supplied by Ordnance Survey from data at 1:1250, 1:2500 and 1:10000 surveying and mapping standards - is used as the primary source. Other sources ? acquired internally and from external suppliers - may include aerial imagery at resolutions ranging from 25cm to 2m, Ordnance Survey 1:10000 raster images, historical OS mapping, charts and chart data from UK Hydrographic Office and other sources, scanned images of paper designation mapping (mostly originally produced at 1:10560 or 1:10000 scales), GPS and other surveyed data, and absolute coordinates. The data was first captured against an August 2002 cut of OS MasterMap Topography. Natural England has successfully uploaded an up-to-date version of OS MasterMap Topographic Layer. However, we have not yet updated our designated data holding to this new version of MasterMap. This should occur in the near future, when we will simultaneously apply positional accuracy improvement (PAI) to our data.",  # noqa: B950
        metadata_date="2020-10-21",
        metadata_language="eng",
        resource_type="dataset",
        file_identifier="c626e031-e561-4861-8219-b04cd1002806",
        quality_scope=("dataset",),
        spatial_representation_type=("vector",),
    )
