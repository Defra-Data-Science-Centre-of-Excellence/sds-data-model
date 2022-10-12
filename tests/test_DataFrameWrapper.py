from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pytest import fixture
from pandas import DataFrame

from sds_data_model.dataframe import DataFrameWrapper

# Create dataframe to test against
expected_data = {'a': [1, 2, 3],
                 'b': [3, 4, 5], 
                 'c': [5, 6, 7]}
expected_df = DataFrame(expected_data)


@fixture
def spark_session() -> SparkSession:
    """Local Spark context."""
    return (
        SparkSession.builder.master(
            "local",
        )
        .appName(
            "Test context",
        )
        .getOrCreate()
    )


@fixture(scope='session')
def temp_file(tmpdir_factory):
    """Create a temporary directory and data for testing

    Args:
        tmpdir_factory (Instance): temporary directory instance

    Returns:
        object: object containing path to temporary data
    """
    p = tmpdir_factory.mktemp("data").join("temp.csv")
    expected_df.to_csv(str(p), index=False, header=True)
    return p


@fixture
def expected_name() -> str:
    """Expected DataFrameWrapper name."""
    return "Trial csv"


@fixture
def expected_metadata() -> None:
    """Expected DataFrameWrapper metadata."""
    return None


@fixture
def expected_schema() -> StructType:
    """Expected DataFrameWrapper schema."""
    return StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
        StructField("c", StringType(), True)
    ])


def test_vector_layer_from_files(
    spark_session: SparkSession,
    temp_file,
    expected_name: str,
    expected_schema: StructType,
    expected_metadata: None
) -> None:
    """Reading test data returns a DataFrameWrapper with expected values."""

    expected_spark = spark_session.createDataFrame(
        expected_df, 
        schema=StructType([
            StructField('a', StringType(), True),
            StructField('b', StringType(), True),
            StructField('c', StringType(), True)
        ]))

    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=str(temp_file),
        read_file_kwargs={'header': True},
        name='Trial csv',
    )

    assert received.name == expected_name
    assert received.metadata == expected_metadata
    assert received.data.schema == expected_schema
    assert_df_equality(received.data, expected_spark)


@fixture
def expected_data_limit(spark_session) -> SparkDataFrame:
    """Expected data when using limit call method."""
    check_limit_data = [
        (1, 2, 3),
        (3, 4, 5)
    ]
    check_limit_spark = spark_session.createDataFrame(check_limit_data, StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
        StructField("c", StringType(), True)
    ]))
    return check_limit_spark


@fixture
def expected_data_select(spark_session) -> SparkDataFrame:
    """Expected data when using select call method."""
    check_select_data = [
        (1, 2),
        (3, 4),
        (5, 6)
    ]
    check_select_spark = spark_session.createDataFrame(check_select_data, StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ]))
    return check_select_spark


@fixture
def expected_data_join(spark_session) -> SparkDataFrame:
    """Expected data when using join call method."""
    check_join_data = [
        (1, 2, 3, 1, 2),
        (3, 4, 5, 3, 4),
        (5, 6, 7, 5, 6)
    ]
    check_join_spark = spark_session.createDataFrame(check_join_data, StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True), 
        StructField("c", StringType(), True),
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ]))

    return check_join_spark


@fixture
def expected_data_filter(spark_session) -> SparkDataFrame:
    """Expected data when using filter call method."""
    check_filter_data = [
        (3, 4, 5)
    ]
    check_filter_spark = spark_session.createDataFrame(check_filter_data, StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True), 
        StructField("c", StringType(), True)
    ]))

    return check_filter_spark


@fixture
def test_call_method(
    spark_session: SparkSession, 
    expected_data_limit,
    expected_data_select,
    expected_data_join, 
    expected_data_filter,
    temp_file
) -> None:
    """Function to test most common methods used by call_method

    Args:
        spark_session (SparkSession): spark_session for test
        expected_data_limit (SparkDataFrame): expected data output for limit method
        expected_data_select (SparkDataFrame): expected data output for select method
        expected_data_join (SparkDataFrame): expected data output for join method
        expected_data_filter (SparkDataFrame): expected data output for filter method
        temp_file (SparkDataFrame): temporary file path for test data
    """

    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=str(temp_file),
        read_file_kwargs={'header': True},
        name='Trial csv',
    )
  
    actual_spark_limit = received.call_method(
        "limit",
        num=2
    )

    actual_spark_select = received.call_method(
        "select",
        [col("a"), col("b")]
    )

    actual_spark_join = received.call_method(
        "join",
        other=expected_data_select
    )

    actual_spark_filter = received.call_method(
        "filter",
        col("a") == 3
    )

    assert_df_equality(actual_spark_limit.data, expected_data_limit)
    assert_df_equality(actual_spark_select.data, expected_data_select)
    assert_df_equality(actual_spark_join.data, expected_data_join)
    assert_df_equality(actual_spark_filter.data, expected_data_filter)