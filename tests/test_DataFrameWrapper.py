"""Tests for DataFrame wrapper class."""

from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union

import pytest
import rioxarray  # noqa: F401
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pytest import FixtureRequest, raises
from xarray import Dataset, open_dataset
from xarray.testing import assert_identical

from sds_data_model.dataframe import DataFrameWrapper
from sds_data_model.raster import DatasetWrapper


def test_vector_layer_from_files(
    spark_session: SparkSession,
    temp_path: str,
    expected_dataframewrapper_name: str,
    expected_dataframe: SparkDataFrame,
    expected_empty_metadata: None,
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )

    assert received.name == expected_dataframewrapper_name
    assert received.metadata == expected_empty_metadata
    assert_df_equality(received.data, expected_dataframe)


@pytest.mark.parametrize(
    argnames=(
        "method_name",
        "method_args",
        "method_kwargs",
        "expected_dataframe_name",
    ),
    argvalues=(
        ("limit", None, {"num": 2}, "expected_dataframe_limit"),
        ("select", ["a", "b"], None, "expected_dataframe_select"),
        ("filter", "a = 3", None, "expected_dataframe_filter"),
    ),
    ids=(
        "limit",
        "select",
        "filter",
    ),
)
def test_call_method(
    spark_session: SparkSession,
    temp_path: str,
    method_name: str,
    method_args: Optional[Union[str, Sequence[str]]],
    method_kwargs: Optional[Dict[str, Any]],
    expected_dataframe_name: str,
    request: FixtureRequest,
) -> None:
    """Function to test most common methods used by call_method."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )

    if method_args and method_kwargs:
        # If we get both, use both, unpacking the `method_kwargs` dictionary.
        received.call_method(method_name, method_args, **method_kwargs)
    elif method_args and isinstance(method_args, list):
        # If we get only method_args, and it's a `list`, use it.
        received.call_method(method_name, *method_args)
    elif method_args:
        # Now method_args has to be a single item, use it.
        received.call_method(method_name, method_args)
    elif method_kwargs:
        # And `method_kwargs` has to be a dictionary, so unpack it.
        received.call_method(method_name, **method_kwargs)
    else:
        # Do nothing
        pass

    expected_dataframe = request.getfixturevalue(expected_dataframe_name)
    assert_df_equality(received.data, expected_dataframe)


def test_call_method_join(
    spark_session: SparkSession,
    temp_path: str,
    dataframe_other: SparkDataFrame,
    expected_dataframe_joined: SparkDataFrame,
) -> None:
    """Passing the `.join` method to `.call_method` produces the expected results."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )
    received.call_method("join", other=dataframe_other, on="a")  # type: ignore[arg-type]  # noqa: B950
    assert_df_equality(received.data, expected_dataframe_joined)


def test_to_zarr(
    dataframe_for_rasterisations: SparkDataFrame,
    tmp_path: Path,
    expected_geometry_mask: Dataset,
) -> None:
    """Rasterisation produces the expected results."""
    dfw = DataFrameWrapper(
        name="geometry_mask",
        data=dataframe_for_rasterisations,
        metadata=None,
    )
    path = tmp_path / "geometry_mask.zarr"
    dfw.to_zarr(path, overwrite=True)
    dsw = DatasetWrapper.from_files(path)
    dsw.dataset = dsw.dataset.chunk(10_000, 10_000)
    assert_identical(dsw.dataset, expected_geometry_mask)


def test_to_zarr_no_metadata(
    hl_zarr_path_no_metadata: str,
    expected_hl_dataset_no_metadata: Dataset,
) -> None:
    """The HL DataFrame wrapper is rasterised as expected."""
    hl_dataset = open_dataset(
        hl_zarr_path_no_metadata,
        engine="zarr",
        decode_coords=True,
        mask_and_scale=False,
        chunks={
            "eastings": 10_000,
            "northings": 10_000,
        },
    )

    assert_identical(hl_dataset, expected_hl_dataset_no_metadata)


def test_to_zarr_with_metadata(
    hl_zarr_path_with_metadata: str,
    expected_hl_dataset_with_metadata: Dataset,
) -> None:
    """Check that attrs in the zarr look as expected."""
    hl_dataset = open_dataset(
        hl_zarr_path_with_metadata,
        engine="zarr",
        decode_coords=True,
        mask_and_scale=False,
        chunks={
            "eastings": 10_000,
            "northings": 10_000,
        },
    )

    assert hl_dataset.attrs == expected_hl_dataset_with_metadata.attrs


@pytest.mark.parametrize(
    argnames=[
        "out_path",
    ],
    argvalues=[
        ("",),
        ("hl.zarr",),
    ],
    ids=[
        "directory",
        ".zarr file",
    ],
)
def test_zarr_overwrite_check(
    out_path: str,
    tmp_path: Path,
    hl_wrapper_no_metadata: DataFrameWrapper,
) -> None:
    """Check error thrown when a zarr output path already contains a zarr."""
    with raises(ValueError, match="Zarr file already exists"):

        hl_wrapper_no_metadata.to_zarr(
            path=str(tmp_path / out_path), data_array_name="tmp_zarr"
        )

        hl_wrapper_no_metadata.to_zarr(
            path=str(tmp_path / out_path), data_array_name="tmp_zarr"
        )
