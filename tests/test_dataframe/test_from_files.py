"""Tests for `DataFrameWrapper.from_files`."""

from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from sds_data_model.dataframe import DataFrameWrapper


def test_from_files(
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
