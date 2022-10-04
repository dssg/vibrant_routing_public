from itertools import combinations
import json
from matplotlib.dates import MonthLocator
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly import offline
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
import yaml

from src.utils.sql_util import get_db_conn, get_metrics_from_db, get_predictions_from_db
from src.utils.pipeline_util import set_prefix_name_of_plot


def plot_scores_vs_actual_labels(
    y_true,
    y_predicted,
    title="",
    plot_prefix_filename="./",
    save_plot=True,
    show_plot=False,
    save_format="png",
):
    """Plot a histogram of the scores compared to the predicted labels.
    The resulting plot will be stored in a html file named plot_prefix_filename concatenated to the name of the function.

    Keyword arguments:
        y_true (1d array) -- values of the true labels.
        y_predicted (1d array) -- values of the predicted labels.
        title (str) -- title for the plot. It defaults to an empty string.
        plot_prefix_filename (str) -- prefix of the name of the plot to store in disk.
                                      It defaults to `./`.
        save_plot (bool) -- indicates whether or not to save plot on disk. It defaults to True.
        show_plot (bool) -- indicates whether the plot should be visualized. It defaults to False.
        save_format (str) -- indicates the format to use to save the plot in disk.
    """
    fig = px.histogram(
        x=y_predicted,
        color=y_true,
        barmode="group",
        labels=dict(color="Actual label", x="Scores"),
        range_x=[0, 1],
        # color_discrete_map={0: "blue", 1: "pink"},
        title=title
        if title != ""
        else "Distribution of the scores vs. the predicted labels",
    ).update_layout(yaxis_title="Number of routed calls")

    if save_plot:
        # Save plot to specified format.
        if save_format == "png":
            fig.write_image(plot_prefix_filename + "plot_scores_vs_actual_labels.png")
        elif save_format == "html":
            offline.plot(
                fig, filename=plot_prefix_filename + "plot_scores_vs_actual_labels.html"
            )
        else:
            logging.warning(f"Unknown format to save plots: {save_format}.")

    # Show the plot if indicated in show_plot.
    if show_plot:
        fig.show()


def compare_inside_model_class(
    db_conn,
    plot_folder_name,
    model_class=None,
    value_of_interest="worst_value",
    save_plot=True,
    show_plot=False,
):
    """Compare the differences in performance for the multiple combination of parameters for the given class.
    Disclaimer: the query is currently hardcoded and we are looking at auc-roc.

    Keyword arguments:
        db_conn (object) -- database connection.
        plot_folder_name (str) -- path to the folder where to store plots in disk.
        model_class (str) -- model class to evaluate in detail. It detaults to NoneType.
        value_of_interest (str) -- value to use to pick the best model.
        save_plot (bool) -- indicates whether or not to save plot on disk. It defaults to True.
        show_plot (bool) -- indicates whether the plot should be visualized. It defaults to False.
    """
    # Set parameters of interest for each model_class.
    if model_class == "ScaledLogisticRegression":
        params = ["C", "penalty"]
    elif model_class in [
        "DecisionTreeClassifier",
        "AdaBoostClassifier",
        "RandomForestClassifier",
        "ExtraTreesClassifier",
    ]:
        params = ["max_depth", "n_estimators"]
    else:
        params = []

    # Get all combination of parameters to draw multiple plots.
    list_params_combinations = []
    for n in np.arange(len(params)):
        list_params_combinations += list(combinations(params, n + 1))

    # Loop over all combination of parameters.
    for params_combination in list_params_combinations:
        # Write the query to combine the parameters of interest.
        # The output of the following line for the case of ScaledLogisticRegression in:
        #     concat('C:', parameters->>'C', '-','penalty:', parameters->>'penalty') as parameters
        query_params = (
            "concat("
            + ", '-',".join([f"'{p}:', parameters->>'{p}'" for p in params_combination])
            + ") as parameters"
        )

        query = f"""
            select train_end_datetime_est, model_class, {query_params if len(params) > 0 else 'parameters'}, max({value_of_interest}) as best_{value_of_interest}
            from experiments.metrics
            left join experiments.models using(model_id)
            where metric = 'auc-roc' and model_class='{model_class}'
            group by train_end_datetime_est, model_class, 3;
            """
        best_models = db_conn.execute(query).fetchall()
        best_models = pd.DataFrame(best_models)

        sns.set(rc={"figure.figsize": (12, 7)})
        ax = sns.lineplot(
            data=best_models,
            x="train_end_datetime_est",
            y=f"best_{value_of_interest}",
            hue="parameters",
            estimator=None,
        )
        ax.set(
            xlabel="Train end datetime [EST]",
            ylabel=f"Best {value_of_interest}",
            title=f"Models with best {value_of_interest} for {model_class} and params:{params_combination}.",
        )

        ax.xaxis.set_major_locator(MonthLocator(interval=4))
        ax.xaxis.set_tick_params(rotation=45)

        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        plt.tight_layout()

        if save_plot:
            plt.savefig(
                f"{plot_folder_name}compare_inside_model_class_{model_class}_params_{str(params_combination)}.png"
            )

        if show_plot:
            plt.show()

        plt.close()


def compare_best_of_models(
    db_conn,
    plot_folder_name,
    value_of_interest="worst_value",
    save_plot=True,
    show_plot=False,
    only_show_best_models=False,
):
    """Compare the best model for each class of models over time.
    Disclaimer: the query is currently hardcoded and we are looking at auc-roc.

    Keyword arguments:
        db_conn (object) -- database connection.
        plot_folder_name (str) -- path to the folder where to store plots in disk.
        value_of_interest (str) -- value to use to pick the best model.
        save_plot (bool) -- indicates whether or not to save plot on disk. It defaults to True.
        show_plot (bool) -- indicates whether the plot should be visualized. It defaults to False.
        only_show_best_models (bool) -- indicates whether there is interest to explore only the best models
                                        of each class or it is preferred to see all available models.
    """
    query = f"""
        select train_end_datetime_est, model_class, parameters, max({value_of_interest}) as best_{value_of_interest}
        from experiments.metrics
        left join experiments.models using(model_id)
        where metric = 'auc-roc'
        group by train_end_datetime_est, model_class, parameters;
        """
    best_models = db_conn.execute(query).fetchall()
    best_models = pd.DataFrame(best_models)

    le = LabelEncoder()
    best_models["parameters"] = le.fit_transform(
        best_models["parameters"].astype("str")
    )

    sns.set(rc={"figure.figsize": (18, 10)})
    ax = sns.lineplot(
        data=best_models,
        x="train_end_datetime_est",
        y=f"best_{value_of_interest}",
        hue="model_class",
        units="parameters",
        estimator=None,
    )
    plt.xlabel("Train end datetime [EST]", fontsize=18)
    plt.ylabel(f"Best {value_of_interest}", fontsize=18)
    plt.title(f"Models with best {value_of_interest}", fontsize=25)

    ax.xaxis.set_major_locator(MonthLocator(interval=4))
    ax.xaxis.set_tick_params(rotation=45)

    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
    plt.tight_layout()

    if save_plot:
        plt.savefig(plot_folder_name + "compare_best_of_models.png")

    if show_plot:
        plt.show()


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling
    with open("config/modeling_config.yaml") as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # For testing purposes: read splits.json.
    with open("config/splits.json") as f:
        splits = json.load(f)

    db_conn = get_db_conn()

    # Set experiment_id and model_id.
    experiment_id = 1
    model_id = 1

    # Get data.
    metrics_data = get_metrics_from_db(
        db_conn=db_conn, experiment_id=experiment_id, model_id=model_id
    )
    predictions_data = get_predictions_from_db(
        db_conn=db_conn, experiment_id=experiment_id, model_id=model_id
    )

    # Set the prefix of the plot filenames.
    plot_prefix_filename = set_prefix_name_of_plot(
        plots_folder_path=modeling_config["plots_folder_path"],
        model_id=model_id,
        experiment_id=experiment_id,
    )

    # Evaluate graphically the predictions.
    plot_scores_vs_actual_labels(
        y_true=predictions_data["y_true"],
        y_predicted=predictions_data["y_predicted"],
        plot_prefix_filename=plot_prefix_filename,
    )
    compare_best_of_models(
        db_conn,
        plot_folder_name=modeling_config["plots_folder_path"],
        show_plot=False,
    )


# main()
