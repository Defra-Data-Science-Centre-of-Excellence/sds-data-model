from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pytest import fixture
from pandas import DataFrame

from sds_data_model.dataframe import DataFrameWrapper

# Create dataframe to test against
expected_data = {'a': [1, 2, 3],
                 'b': [3, 4, 5]}
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
        tmpdir_factory (_type_): temporary directory instance

    Returns:
        _type_: object containing path to temporary data 
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
        StructField("b", StringType(), True)
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
            StructField('b', StringType(), True)
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
    """Expected DataFrameWrapper schema."""
    check_limit_data = [
    (1,2),
    (2,3)
    ]
    check_limit_spark = spark_session.createDataFrame(check_limit_data, StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ]))
    return check_limit_spark


@fixture
def test_call_method(
    spark_session: SparkSession, 
    expected_data_limit, 
    temp_file
) -> None:

    received = DataFrameWrapper.from_files(
        spark=spark_session,
        data_path=str(temp_file),
        read_file_kwargs={'header': True},
        name='Trial csv',
    )
    
    actual_spark = DataFrameWrapper.call_method(
        received,
        method_name='limit',
        num=2)

    assert_df_equality(actual_spark.data, expected_data_limit)

        
