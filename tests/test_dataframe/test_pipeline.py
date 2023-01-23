"""Tests for the `DataFrameWrapper` pipeline."""
from pathlib import Path

from chispa import assert_df_equality
from shapely.wkb import loads
from shapely.geometry.polygon import orient
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import udf

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
    ).call_method(
        "withColumn", "geometry", udf(lambda x: orient(loads(bytes(x))).wkt)("geometry")
    )

    assert_df_equality(dfw.data, spark_dataframe)
