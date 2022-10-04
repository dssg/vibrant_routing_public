import logging
import json
import yaml

from config.project_constants import MODELING_CONFIG_FILE

from src.pipeline.call.matrix_creator import matrix_creator
from src.pipeline.call.model_trainer import all_models_trainer
from src.utils.pipeline_util import split_features_label
from src.utils.sql_util import (
    get_db_conn,
    add_predictions_to_db,
    get_experiment_id,
)
import click


def predict(
    db_conn,
    matrix,
    matrix_file_path,
    model,
    model_id,
    split,
    label_column_name,
    columns_to_remove=None,
    pos_label=1,
):
    """Predict on the test set based on the trained model and store the predictions in the database.

    Keyword arguments:
        db_conn (object) -- database connection.
        matrix (pd.DataFrame) -- dataset containing both features and labels together.
        matrix_file_path (str) -- file path where the matrix is stored.
        model (object) -- trained model.
        model_id (int) -- model identifier.
        split (dict) -- dictionary with the split information.
        label_column_name (str) -- name of label column in the dataset matrix.
        columns_to_remove (list[str]) -- list of columns to remove from dataset matrix for training.
                                      Defaults to NoneType.
        pos_label (int) -- the label of the positive class.

    Returns:
        y_pred (1d array) -- predicted probability.
    """
    model_parent_dir = model.__module__.rsplit(".", 2)[1]

    if model_parent_dir == "baseline":
        # The baseline requires the datetime split for the matrix.
        kwargs = split
    else:
        kwargs = {}

    X, y = split_features_label(
        matrix=matrix,
        label_column_name=label_column_name,
        columns_to_remove=columns_to_remove,
    )

    y_pred = model.predict_proba(X, **kwargs)[:, pos_label]
    experiment_id = get_experiment_id(
        db_conn=db_conn,
        model_id=model_id,
        split=split,
        evaluation_matrix_path=matrix_file_path,
    )

    add_predictions_to_db(
        experiment_id=experiment_id,
        model_id=model_id,
        y_index=matrix["routing_attempts_id"],
        y_true=y,
        y_predicted=y_pred,
    )

    return y_pred


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

    # Get database connection.
    db_conn = get_db_conn()

    # For testing purposes: read splits.json.
    with open(splits) as f:
        splits = json.load(f)

    label_column_name = modeling_config["matrix_creator_config"]["label_column_name"]
    columns_to_remove = modeling_config["matrix_creator_config"]["columns_to_remove"]

    matrix_file_path, matrix = matrix_creator(
        db_conn=db_conn,
        split=splits[0]["validation"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )

    model_ids, models = all_models_trainer(
        db_conn=db_conn,
        matrix=matrix,
        model_grid=modeling_config["model_grid"],
        split=splits[0]["validation"],
        label_column_name=label_column_name,
        model_folder_path=modeling_config["model_folder_path"],
        train_matrix_file_path=matrix_file_path,
        columns_to_remove=columns_to_remove,
        log_path="",
    )

    for model_id, model in zip(model_ids, models):
        prediction = predict(
            db_conn=db_conn,
            matrix=matrix,
            matrix_file_path=matrix_file_path,
            model=model,
            model_id=model_id,
            split=splits[0]["validation"],
            label_column_name=label_column_name,
            columns_to_remove=columns_to_remove,
        )

        logging.debug(f"The predictions is {prediction}.")


# main()
