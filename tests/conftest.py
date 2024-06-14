import pytest
from pyspark.sql import Row, SparkSession


@pytest.fixture(scope="module")
def spark_session():
    try:
        spark = SparkSession.builder.master("local[*]").getOrCreate()

        yield spark
    finally:
        spark.stop()


@pytest.fixture
def mock_df(spark_session):
    return spark_session.createDataFrame(
        [
            Row(col=0),
        ]
    )


@pytest.fixture
def df_with_const_cols(spark_session):
    return spark_session.createDataFrame(
        [
            Row(id=1, col1=10, col2=0, col3=1),
            Row(id=2, col1=20, col2=0, col3=1),
        ]
    )
