from pathlib import Path
from typing import ContextManager, Optional, Tuple, Union

import pytest
from _pytest.fixtures import FixtureRequest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pytest import raises
import pandas as pd

# def test__create_paths_sdf(
#     spark_context: SparkSession,
#     name: str,
#     expected_name: SparkDataFrame,
# ) -> None:
#     """Returns the expected SparkDataFrame of paths."""
#     paths_sdf = _create_paths_sdf(
#         spark=spark_context,
#         paths=(first_fileGDB_path,),
#     )
#     assert_df_equality(paths_sdf, expected_paths_sdf)

def test_call_method(
    spark_context: SparkSession,
    #method_name: str, 
) -> None:
    """_summary_

    Args:
        spark_context (SparkSession): _description_
        method_name (str): _description_
    """
    expected_data = {'a':[1,2],
       'b':[3,4]}
    expected_df= pd.DataFrame(expected_data)
    sparkDF =spark_context.createDataFrame(expected_df)
    edit_data = {'a':[1,2,3],
        'b':[3,4,5]}

    edit_df= pd.DataFrame(edit_data)
    sparkDF_edit =spark_context.createDataFrame(edit_df)
    sparkDF_edited = sparkDF_edit.limit(2)

    assert_df_equality(sparkDF, sparkDF_edited)