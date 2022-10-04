import logging
import numpy as np
import pandas as pd

from src.utils.sql_util import add_feature_importance_to_db


def split_features_label(matrix, label_column_name=None, columns_to_remove=None):
    """Split matrix into features and label.

    Keyword arguments:
        matrix (pd.DataFrame) -- dataset containing both features and labels together.
        label_column_name (str) -- name of label column in the dataset matrix.
                                    Defaults to NoneType.
        columns_to_remove (list[str]) -- list of columns to remove from dataset matrix for training.
                                      Defaults to NoneType.

    Returns:
        (features (pd.DataFrame), label (1d array)) -- tuple with the features and the label.
    """

    column_names = list(matrix.columns)

    if label_column_name is not None:
        # Ensure that label_name is valid.
        assert (
            label_column_name in column_names
        ), f"Invalid `label_name`. There is no column called {label_column_name} in dataset matrix!"

        # Remove the label from list of columns.
        column_names.remove(label_column_name)
        label = matrix.loc[:, label_column_name].values.flatten()
    else:
        label = np.array([])

    if columns_to_remove is not None:
        feature_list = [i for i in column_names if i not in columns_to_remove]
    else:
        feature_list = column_names

    features = matrix.loc[:, feature_list]

    return features, label


def set_prefix_name_of_plot(plots_folder_path, model_id, experiment_id):
    """Set the name of the model based on the model_type and model_folder_path.

    Keyword arguments:
        plots_folder_path (str) -- folder where the plot should be stored.
        model_id (int) -- identifier of the model.
        experiment_id (int) -- identifier of the experiment.

    Returns:
        plot_prefix_filename (str) -- prefix of the path where the model should be stored.
                                      It only returns the prefix since the name is completed inside
                                      the corresponding plot functions.
    """
    plot_prefix_filename = (
        plots_folder_path
        + "model"
        + str(model_id)
        + "_experiment"
        + str(experiment_id)
        + "_"
    )
    return plot_prefix_filename


def complete_query_fillings_for_skeleton(feature_family_values):
    """Generate the complete query filling for query skeleton for feature creation.

    Keyword arguments:
        feature_family_values (dict) -- dictionary of query filling name and content.

    Returns:
        complete_query_filling (str) -- string of complete query filling with individual query filling connected together with commas.
    """

    # Initialize the query filling.
    base_query_filling = feature_family_values["query_filling"]

    # Check the config file for parameters
    if "parameter_1" in feature_family_values.keys():
        parameter_1 = feature_family_values["parameter_1"]
    else:
        parameter_1 = None

    if "parameter_2" in feature_family_values.keys():
        parameter_2 = feature_family_values["parameter_2"]
    else:
        parameter_2 = None

    if "parameter_3" in feature_family_values.keys():
        parameter_3 = feature_family_values["parameter_3"]
    else:
        parameter_3 = None

    # Generate the whole query for that family.
    complete_query_filling = ""

    # Append one feature at a time to the query.
    # If this feature has no parameters to buid upon
    if not parameter_1:
        logging.debug(f"Creating feature table with zero params.")
        complete_query_filling += f"{base_query_filling},"

        if parameter_2 or parameter_3:
            logging.warning(
                f"parameter_1 list is empty but parameter_2 or parameter_3 list is not: parameter_2={parameter_2}, parameter_3={parameter_3}. Features will not be built on parameter_2."
            )
    else:
        # If this feature has only one param (i.e. parameter_1) to build upon
        if not parameter_2:
            logging.debug(f"Creating feature table with one param: {parameter_1}")
            for p_1 in parameter_1:
                complete_query_filling += (
                    f"{base_query_filling.format(parameter_1=p_1)},"
                )
        # If this feature has two params (i.e. parameter_1 and parameter_2) to build upon:
        elif not parameter_3:
            logging.debug(
                f"Creating feature table with two params: {parameter_1} and {parameter_2}"
            )
            for p_2 in parameter_2:
                for p_1 in parameter_1:
                    complete_query_filling += f"{base_query_filling.format(parameter_1=p_1, parameter_2=p_2)},"
        # If this feature has three params logging(i.e. parameter_1 and parameter_2 and parameter_3) to build upon:
        else:
            logging.debug(
                f"Creating feature table with three params: {parameter_1} and {parameter_2} and {parameter_3}"
            )
            for p3 in parameter_3:
                for p_2 in parameter_2:
                    for p_1 in parameter_1:
                        complete_query_filling += f"{base_query_filling.format(parameter_1=p_1, parameter_2=p_2, parameter_3=p3)},"

    # Splice off the last trailing comma and extra newline.
    complete_query_filling = complete_query_filling.strip(",").strip()
    return complete_query_filling


def feature_importance(model, model_id, column_names):
    """Gets the ranking of how important each feature is to the model's performance.
    The currently considered `models` are trees and regression models.

    Keyword arguments:
        model (object) -- the model of interest.
        model_id (int) -- the identifier of the model of interest.
        column_names (list) -- list of columns, which are also the feature names.

    Raises:
        NotImplementedError -- if the model does not have an attribute with `imp` or `coef_`.

    Returns:
        feature_rankings (pd.DataFrame) -- contains two columns: `cols`, and either `imp` or `abs_coef`.
                                            `imp` if `model` has feature_importances_
                                            `abs_coef` if `model` has `coef_`
    """
    feature_rankings = None
    try:
        imp = model.feature_importances_
        feature_rankings = pd.DataFrame(
            {
                "model_id": [model_id] * len(column_names),
                "feature_name": column_names,
                "feature_importance": imp,
            }
        ).sort_values("feature_importance", ascending=False)
    except:
        try:
            abs_coef = model.coef_[0]
            feature_rankings = pd.DataFrame(
                {
                    "model_id": [model_id] * len(column_names),
                    "feature_name": column_names,
                    "feature_importance": abs(abs_coef),
                }
            ).sort_values("feature_importance", ascending=False)
        except:
            logging.error(
                f"model_id:{model_id} does not have `feature_importances_` or `coef_` method."
            )

    # Add the importance of features in the db.
    if feature_rankings is not None:
        add_feature_importance_to_db(feature_rankings=feature_rankings)

    return feature_rankings
