"""Tests for the `DataFrameWrapper` pipeline."""
from pathlib import Path

from chispa import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame

from sds_data_model.dataframe import DataFrameWrapper


def test_pipeline(
    tmp_path: Path,
    spark_dataframe: SparkDataFrame,
) -> None:
    """End to end test."""
    dfw = DataFrameWrapper.from_files(
        data_path=str(tmp_path),
        read_file_kwargs={
            "suffix": ".shp",
        },
        name="test",

    )

    assert_df_equality(dfw.data, spark_dataframe)
