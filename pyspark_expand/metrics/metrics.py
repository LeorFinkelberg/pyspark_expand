import typing as t

from pyspark.ml.evaluation import RegressionEvaluator


def adjusted_r2(
    dataset: "DataFrame",
    label_col: str = "label",
    feature_col: str = "features",
    prediction_col: str = "prediction",
    params: t.Optional[t.Dict] = None,
) -> float:
    """
    Computes adjusted R^2.

    R^2 is a biased estimate since
    the number of features influences the value.
    When you add a noisy feature or completely
    random feature, the R^2 value increases. Therefore,
    it is good to adjust R^2 value based on the number of
    features in the model.

    Parameters
    ----------
    dataset: :py:class:`pyspark.sql.DataFrame`
        A dataset that contains labels/observations and predictions
    label_col: str, optional
        Target column name (default: "label")
    feature_col: str, optional
        Feature names (default: "features")
    prediction_col: str, optional
        Prediction column name (default: "prediction")
    params: dict, optional
        An optional param map that overrides embedded params

    Returns
    -------
    float
        metric
    """

    _METRIC_NAME = "r2"

    evaluator = RegressionEvaluator(
        labelCol=label_col,
        predictionCol=prediction_col,
        metricName=_METRIC_NAME,  # noqa
    )

    r2 = evaluator.evaluate(dataset, params=params)
    n = dataset.count()
    p = len(dataset.select(feature_col).first().asDict()[feature_col])

    return 1 - ((1 - r2**2) * (n - 1) / (n - p - 1))
