import pytest
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.testing.utils import assertDataFrameEqual

from pyspark_expand.common import drop_cols_with_set_cardinality
from pyspark_expand.common.exceptions import *


@pytest.mark.parametrize("cardinality", [0, -1])
def test_drop_cols_with_non_positive_cardinality(
    mock_df,
    cardinality: int,
):
    df: SparkDataFrame = mock_df

    with pytest.raises(InvalidCardinalityError):
        drop_cols_with_set_cardinality(df, cardinality)


def test_drop_cols_positive_cardinality(df_with_const_cols):
    cols_with_1_cardinality = ["col2", "col3"]
    actual = drop_cols_with_set_cardinality(df_with_const_cols)
    expected = df_with_const_cols.drop(*cols_with_1_cardinality)

    assertDataFrameEqual(actual, expected)
