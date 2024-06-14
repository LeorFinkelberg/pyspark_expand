import typing as t

from pyspark.sql import functions as F

from pyspark_expand.common.exceptions import *


def assert_never(_: t.NoReturn) -> t.NoReturn:
    """
    Special function to help
    static type checking system
    """

    raise AssertionError("Error! Expected code to be unreachable ...")


def drop_cols_with_set_cardinality(
    df: "DataFrame",
    cardinality: int = 1,
) -> "DataFrame":
    """
    Drops columns with set cardinality.

    Parameters
    ---------
    df: :class:`DataFrame`
        Source DataFrame.
    cardinality: int, optional
        Cardinality of the column to drop (default: 1).

    Returns
    -------
    :class:`DataFrame`
        DataFrame without columns with a set cardinality.

    Examples
    --------
    >>> from pyspark.sql import Row, functions as F
    >>> from pyspark_expand.common import drop_cols_with_set_cardinality
    >>> df = spark.createDataFrame([
    ...     Row(id=1, col1=10, col2=0),
    ...     Row(id=2, col1=20, col2=0),
    ... ])
    >>> drop_cols_with_set_cardinality(df).show()
    +---+----+
    | id|col1|
    +---+----+
    |  1|  10|
    |  2|  20|
    +---+----+
    """

    if cardinality <= 0:
        raise InvalidCardinalityError(
            "Cardinality must be positive. " f"Passed value: {cardinality}"
        )

    return df.drop(
        *[
            key
            for key, value in df.select(
                [F.approx_count_distinct(col_).alias(col_) for col_ in df.columns]
            )
            .first()
            .asDict()
            .items()
            if value == cardinality
        ]
    )
