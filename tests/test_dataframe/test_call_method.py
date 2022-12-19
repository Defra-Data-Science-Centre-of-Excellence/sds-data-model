"""Tests for `DataFrameWrapper.call_method`."""

from typing import Any, Dict, Optional, Sequence, Union

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pytest import FixtureRequest

from sds_data_model.dataframe import DataFrameWrapper


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
        ("show", None, None, "expected_dataframe"),
    ),
    ids=(
        "limit",
        "select",
        "filter",
        "show",
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


def test_call_method_groupBy(
    spark_session: SparkSession,
    temp_path: str,
    expected_dataframe_grouped: SparkDataFrame,
) -> None:
    """Passing the `.agg` method to `.call_method` produces the expected results."""
    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=temp_path,
        read_file_kwargs={"header": True, "inferSchema": True},
        name="Trial csv",
    )
    received.call_method("groupBy", "category").call_method("avg")  # type: ignore[arg-type]  # noqa: B950
    assert_df_equality(received.data, expected_dataframe_grouped)
