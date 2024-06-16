from pyspark.ml.evaluation import RegressionEvaluator


def adjusted_r2(
    pred: "DataFrame",
    label_col: str = "label",
    feature_col: str = "features",
    prediction_col: str = "prediction",
) -> float:
    """
    Compute adjusted R^2.

    R^2 is a biased estimate since
    the number of features influences the value.
    When you add a noisy feature or completely
    random feature, the R^2 value increases. Therefore,
    it is good to adjust R^2 value based on the number of
    features in the model.

    :return:
    """
    METRIC_NAME = "r2"

    evaluator = RegressionEvaluator(
        labelCol=label_col,
        predictionCol=prediction_col,
        metricName=METRIC_NAME,  # noqa
    )
    r2 = evaluator.evaluate(pred)
    N = pred.count()
    p = len(pred.select(feature_col).first().asDict()[feature_col])

    return 1 - ((1 - r2**2) * (N - 1) / (N - p - 1))
