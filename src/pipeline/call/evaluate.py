import glob
import json
import numpy as np
import joblib
import os
from sklearn import metrics
import yaml
import click

from config.project_constants import MODELING_CONFIG_FILE

from src.utils.sql_util import (
    get_db_conn,
    get_experiment_id,
    add_metric_entry_to_db,
    get_model_id_from_path,
    get_predictions_from_db,
)

from src.utils.metric_util import (
    sort_predictions_and_labels,
    process_pct_or_abs,
    true_positives,
    false_positive_rate,
    check_if_metric_exists,
)


def evaluate(
    db_conn,
    model_id,
    split,
    y_true,
    y_predicted,
    validation_matrix_file_path,
    metrics_to_evaluate=["auc-roc"],
    n_stochastic_experiments=10,
):
    """Compare a given prediction (y_predicted) with its actual values (y_true).

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- identifier of the entry created in the database for model governance.
        split (dict) -- dictionary with the split ends datetime.
        y_true (1d array) -- values of the true labels.
        y_predicted (1d array) -- values of the predicted labels.
        validation_matrix_file_path (str) -- file path where the validation matrix is stored for model governance.
        metrics_to_evaluate (list) -- list of metrics to evaluate the predictions on.
                                      Example: [
                                        {'metrics': ['auc-roc']},
                                        {
                                            'metrics': ['precision@'],
                                            'thresholds':
                                                {
                                                    'PERCENTILES': [0, 10, 20, 30, 40, 50, 60, 70, 80, 90],
                                                    'TOP_N': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                                                }
                                            }
                                        ]
        n_stochastic_experiments (int) -- number of experiments needed to compute the stochastic value.

    Returns:
        dict_metrics (dict) -- dictionary containing the metrics as requested in the
                               metrics_to_evaluate attribute, along with their scores.
    """
    # Check if any requested metric is not present in the list of metrics designed.
    validation_metrics = ["auc-roc", "precision@", "recall@", "fpr@"]
    check_if_metric_exists(metrics_to_evaluate, validation_metrics)

    # Create the dictionary to output.
    dict_metrics = {}

    # Compute the preset metrics.
    for group_of_metrics_to_evaluate in metrics_to_evaluate:
        for metric_to_evaluate in group_of_metrics_to_evaluate["metrics"]:
            for tie_breaker in [
                "worst_value"
            ]:  # "best_value", "worst_value", "stochastic_value"]:
                # Calculate sorted values for y_true and y_predicted.
                y_true_sorted, y_pred_sorted = sort_predictions_and_labels(
                    y_true=y_true, y_pred=y_predicted, tie_breaker=tie_breaker
                )

                # Calculate AUC-ROC.
                if metric_to_evaluate == "auc-roc":
                    if metric_to_evaluate not in dict_metrics.keys():
                        dict_metrics[metric_to_evaluate] = {}

                    if tie_breaker == "stochastic_value":
                        list_values = []
                        for _ in np.arange(n_stochastic_experiments):
                            (
                                y_true_sorted,
                                y_pred_sorted,
                            ) = sort_predictions_and_labels(
                                y_true=y_true,
                                y_pred=y_predicted,
                                tie_breaker=tie_breaker,
                            )
                            fpr, tpr, _ = metrics.roc_curve(
                                y_true_sorted, y_pred_sorted
                            )
                            list_values.append(metrics.auc(fpr, tpr))
                        dict_metrics[metric_to_evaluate]["stochastic_value"] = np.mean(
                            list_values
                        )
                        dict_metrics[metric_to_evaluate]["stochastic_std"] = np.std(
                            list_values
                        )
                    else:
                        fpr, tpr, _ = metrics.roc_curve(y_true_sorted, y_pred_sorted)
                        dict_metrics[metric_to_evaluate][tie_breaker] = metrics.auc(
                            fpr, tpr
                        )
                # Calculate threshold-based metrics.
                elif metric_to_evaluate in ["precision@", "recall@", "fpr@"]:
                    # Evaluate if there are thresholds specified to compute the metric at different thresholds.
                    assert (
                        "thresholds" in group_of_metrics_to_evaluate
                    ), f"{metric_to_evaluate} needs the specification of a threshold."

                    thresholds = group_of_metrics_to_evaluate["thresholds"]

                    # Evaluate the metric at different threholds if those were indicated.
                    for threshold_type in thresholds.keys():
                        for k in thresholds[threshold_type]:
                            # Calculate processed values for y_true and y_pred according to treshold_type.
                            y_true_k, y_pred_k = process_pct_or_abs(
                                y_true=y_true_sorted,
                                y_predicted=y_pred_sorted,
                                threshold_type=threshold_type,
                                k=k,
                            )

                            # Compute the specified metric name for the given threshold.
                            metric_name = (
                                metric_to_evaluate + threshold_type + "=" + str(k)
                            )
                            if metric_name not in dict_metrics.keys():
                                dict_metrics[metric_name] = {}

                            # Calculate precision@.
                            if metric_to_evaluate == "precision@":
                                if tie_breaker == "stochastic_value":
                                    list_values = []
                                    for _ in np.arange(n_stochastic_experiments):
                                        y_true_k, y_pred_k = process_pct_or_abs(
                                            y_true=y_true_sorted,
                                            y_predicted=y_pred_sorted,
                                            threshold_type=threshold_type,
                                            k=k,
                                            tie_breaker=tie_breaker,
                                        )
                                        list_values.append(
                                            metrics.precision_score(
                                                y_true=y_true_k,
                                                y_pred=y_pred_k,
                                                zero_division=0,
                                            )
                                        )
                                    dict_metrics[metric_name][
                                        "stochastic_value"
                                    ] = np.mean(list_values)
                                    dict_metrics[metric_name][
                                        "stochastic_std"
                                    ] = np.std(list_values)

                                else:
                                    dict_metrics[metric_name][
                                        tie_breaker
                                    ] = metrics.precision_score(
                                        y_true=y_true_k,
                                        y_pred=y_pred_k,
                                        zero_division=0,
                                    )
                            # Calculate recall@.
                            elif metric_to_evaluate == "recall@":
                                if tie_breaker == "stochastic_value":
                                    list_values = []
                                    for _ in np.arange(n_stochastic_experiments):
                                        y_true_k, y_pred_k = process_pct_or_abs(
                                            y_true=y_true_sorted,
                                            y_predicted=y_pred_sorted,
                                            threshold_type=threshold_type,
                                            k=k,
                                            tie_breaker=tie_breaker,
                                        )
                                        list_values.append(
                                            true_positives(
                                                y_true=y_true_k,
                                                y_pred=y_pred_k,
                                            )
                                            / sum(y_true)
                                        )
                                    dict_metrics[metric_name][
                                        "stochastic_value"
                                    ] = np.mean(list_values)
                                    dict_metrics[metric_name][
                                        "stochastic_std"
                                    ] = np.std(list_values)
                                else:
                                    dict_metrics[metric_name][
                                        tie_breaker
                                    ] = true_positives(
                                        y_true=y_true_k,
                                        y_pred=y_pred_k,
                                    ) / sum(
                                        y_true
                                    )
                            # Calculate fpr@.
                            elif metric_to_evaluate == "fpr@":
                                if tie_breaker == "stochastic_value":
                                    list_values = []
                                    for _ in np.arange(n_stochastic_experiments):
                                        y_true_k, y_pred_k = process_pct_or_abs(
                                            y_true=y_true_sorted,
                                            y_predicted=y_pred_sorted,
                                            threshold_type=threshold_type,
                                            k=k,
                                            tie_breaker=tie_breaker,
                                        )

                                        list_values.append(
                                            false_positive_rate(
                                                y_true=y_true_k,
                                                y_pred=y_pred_k,
                                            )
                                        )
                                    dict_metrics[metric_name][
                                        "stochastic_value"
                                    ] = np.mean(list_values)
                                    dict_metrics[metric_name][
                                        "stochastic_std"
                                    ] = np.std(list_values)
                                else:
                                    dict_metrics[metric_name][
                                        tie_breaker
                                    ] = false_positive_rate(
                                        y_true=y_true_k,
                                        y_pred=y_pred_k,
                                    )
                            else:
                                continue
                # Skip if metric_to_evaluate is not included in the precalculated options.
                else:
                    continue

    # Add experiment entry to experiments.evaluations.
    experiment_id = get_experiment_id(
        db_conn=db_conn,
        model_id=model_id,
        split=split,
        evaluation_matrix_path=validation_matrix_file_path,
    )

    # Add metrics entries to experiments.metrics.
    add_metric_entry_to_db(
        db_conn=db_conn,
        experiment_id=experiment_id,
        model_id=model_id,
        evaluation_metrics=dict_metrics,
    )

    return dict_metrics


@click.command()
@click.option(
    "--splits",
    prompt="Path to the .json file containing training/validation splits info",
    default="config/splits.json",
)
def main(splits):
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # For testing purposes: read splits.json.
    with open(splits) as f:
        splits = json.load(f)

    # Read latest available model.
    list_of_available_models = glob.glob(
        str(modeling_config["model_folder_path"] + "*.pickle")
    )
    latest_model = max(list_of_available_models, key=os.path.getctime)
    print(latest_model)

    model = joblib.load(latest_model)

    db_conn = get_db_conn()

    # Get experiment_id and model_id.
    model_id = get_model_id_from_path(db_conn=db_conn, pickle_path=latest_model)
    experiment_id = get_experiment_id(
        db_conn=db_conn,
        model_id=model_id,
        split=splits[0]["validation"],
        evaluation_matrix_path="",
    )

    # Get predictions.
    predictions_data = get_predictions_from_db(
        db_conn=db_conn, experiment_id=experiment_id, model_id=model_id
    )

    # Evaluate the trained model.
    evaluation_metrics = evaluate(
        db_conn=db_conn,
        model_id=get_model_id_from_path(db_conn=db_conn, pickle_path=latest_model),
        split=splits[0]["validation"],
        y_true=predictions_data["y_true"].to_list(),
        y_predicted=predictions_data["y_predicted"].to_list(),
        validation_matrix_file_path="",
        metrics_to_evaluate=modeling_config["scoring"],
    )


# main()
