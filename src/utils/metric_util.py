from operator import itemgetter
import random
import logging
import numpy as np
import pandas as pd
from sklearn import metrics


def false_positive_rate_at_threshold(y_true, y_predicted, threshold):
    """Calculate false positive rate at a specified threshold.

    Keyword arguments:
        y_true (1d array) -- values of the true labels.
        y_predicted (1d array) -- values of the predicted labels.
        threshold (float) -- threshold that will define how to convert the predicted score to a binary outcome.

    Returns:
        (float) -- false positive rate.
    """
    tn = true_negatives(y_true=y_true, y_pred=y_predicted >= threshold)
    fp = false_positives(y_true=y_true, y_pred=y_predicted >= threshold)

    if (tn + fp) == 0:
        return 0
    else:
        return fp / (fp + tn)


def false_positive_rate(y_true, y_pred):
    """Calculate FPR. The metric assumes a thresholded `y_pred`.

    Keyword arguments:
        y_true (1d array) -- binary values of the true labels.
        y_pred (1d array) -- binary values of the predicted labels.

    Returns:
        (float) -- false positive rate.
    """
    tn = true_negatives(y_true=y_true, y_pred=y_pred)
    fp = false_positives(y_true=y_true, y_pred=y_pred)

    if (tn + fp) == 0:
        return 0
    else:
        return fp / (fp + tn)


def true_positive_rate(y_true, y_pred):
    """Calculate TPR. The metric assumes a thresholded `y_pred`.

    Keyword arguments:
        y_true (1d array) -- binary values of the true labels.
        y_pred (1d array) -- binary values of the predicted labels.

    Returns:
        (float) -- true positive rate.
    """
    tp = true_positives(y_true=y_true, y_pred=y_pred)
    fn = false_negatives(y_true=y_true, y_pred=y_pred)

    if (tp + fn) == 0:
        return 0
    else:
        return tp / (tp + fn)


def true_positives(y_true, y_pred):
    """Calculate the number of true positives given y_true and y_pred."""
    # If all y_true = 1 and all y_pred = 1.
    if all(y_true) and all(y_pred):
        return len(y_true)
    # If all y_true = 0.
    elif not any(y_true):
        return 0
    # Otherwise calculate through the sklearn preset function.
    else:
        return int(metrics.confusion_matrix(y_true=y_true, y_pred=y_pred)[1, 1])


def true_negatives(y_true, y_pred):
    """Calculate the number of true negatives given y_true and y_pred."""
    # If all y_true = 1.
    if all(y_true):
        return 0
    # If all y_true = 0 and y_pred = 0.
    elif not any(y_true) and not any(y_pred):
        return 1
    # Otherwise calculate through the sklearn preset function.
    else:
        return int(metrics.confusion_matrix(y_true=y_true, y_pred=y_pred)[0, 0])


def false_positives(y_true, y_pred):
    """Calculate the number of false positives given y_true and y_pred."""
    # If all y_true = 1 and all y_pred = 1.
    if all(y_true) and all(y_pred):
        return 0
    # If all y_true = 0.
    elif not any(y_true):
        return np.count_nonzero(y_pred == 1)
    # Otherwise calculate through the sklearn preset function.
    else:
        return int(metrics.confusion_matrix(y_true=y_true, y_pred=y_pred)[1, 0])


def false_negatives(y_true, y_pred):
    """Calculate the number of false negatives given y_true and y_pred."""
    # If all y_true = 1 and all y_pred = 1.
    if all(y_true) and all(y_pred):
        return 0
    # If all y_true = 0.
    elif not any(y_true):
        return np.count_nonzero(y_pred == 1)
    # Otherwise calculate through the sklearn preset function.
    else:
        return int(metrics.confusion_matrix(y_true=y_true, y_pred=y_pred)[0, 1])


def sort_predictions_and_labels(
    y_true, y_pred, tie_breaker="stochastic_value", sort_seed=0
):
    """Sort y_pred and y_true with a configured tiebreaking rule.

    Keyword arguments:
        y_true (1d array) -- values of the true labels.
        y_predicted (1d array) -- values of the predicted labels.
        tie_breaker (str) -- the tie breaking method should be one of ["best_value", "worst_value", "stochastic_value"].
        sort_seed (int) -- the sort seed needed if "stochastic" tiebreaking is picked. It defaults to 0.

    Returns:
        (y_true, y_pred) (tuple) -- values of y_true and y_pred sorted according to the specified tie_breaker method.
    """
    if len(y_true) == 0:
        logging.warning("No y_true present, skipping predictions sorting.")
        return (y_pred, y_true)

    df = pd.DataFrame(y_pred, columns=["score"])
    df["y_true"] = y_true

    if tie_breaker == "best_value":
        df.sort_values(
            by=["score", "y_true"],
            inplace=True,
            ascending=[False, False],
            na_position="last",
        )
    elif tie_breaker == "worst_value":
        df.sort_values(
            by=["score", "y_true"],
            inplace=True,
            ascending=[False, True],
            na_position="first",
        )
    elif tie_breaker == "stochastic_value":
        random.seed(sort_seed)
        np.random.seed(sort_seed)
        df["stochastic"] = np.random.rand(len(df))
        df.sort_values(
            by=["score", "stochastic"], inplace=True, ascending=[False, False]
        )
        df.drop("stochastic", axis=1)
    else:
        logging.warning(f"Unknown tie_breaker: {tie_breaker}.")

    return df["y_true"].to_numpy(), df["score"].to_numpy()


def process_pct_or_abs(y_true, y_predicted, threshold_type, k, tie_breaker=None):
    """Process y_true and y_predicted to get values for percentiles or absolute threshold calculations.
    If tie_breaker is not None, the function sort_predictions_and_labels is called to sort the output values
    according to the tie_breaker method.

    Keyword arguments:
        y_true (1d array) -- values of the true labels.
        y_predicted (1d array) -- values of the predicted labels.
        threshold_type (str) -- the threshold type should be one of ["pct", "abs"].
        k (float) -- threshold value.
        tie_breaker (str) -- the tie breaking method should be one of [best_value", "worst_value", "stochastic_value"].
                            It defaults to NoneType. If it has a value, the function sort_predictions_and_labels
                            is called to sort the y_true and y_pred processed values.

    Returns:
        (y_true, y_pred) (tuple) -- values of y_true and y_pred processed according to the specified threshold type.
    """
    if tie_breaker:
        y_true, y_predicted = sort_predictions_and_labels(
            y_true=y_true, y_pred=y_predicted, tie_breaker=tie_breaker
        )

    # Process for percentiles.
    if threshold_type == "pct":
        sample_size = int(np.round(k * len(y_predicted)))
        y_predicted = np.zeros_like(y_predicted)
        y_predicted[:sample_size] = 1
        return y_true, y_predicted

    # Process for absolute thresholds.
    elif threshold_type == "abs":
        return y_true, y_predicted >= k
    else:
        logging.error(f"Unknown threshold_type {threshold_type}.")

    # Raise error if type of threshold is unknown.
    logging.error(f"Unknown threshold type: {threshold_type}.")


def check_if_metric_exists(metrics_to_evaluate, validation_metrics):
    """Check that `metrics_to_evaluate` exists in `validation_metrics`"""
    # Compute the list of metrics whose logic is not implemented in the evaluate function.
    # The following loop iterates through all the metrics specified in the metrics_to_evaluate attribute.
    invalid_metrics = [
        metric_to_evaluate
        for metric_to_evaluate in sum(
            list(map(itemgetter("metrics"), metrics_to_evaluate)), []
        )
        if metric_to_evaluate not in validation_metrics
    ]

    if len(invalid_metrics) > 0:
        logging.warning(
            f"The following metrics are invalid and thus not present in the list of metrics designed to evaluate the predictions: {invalid_metrics}."
        )
