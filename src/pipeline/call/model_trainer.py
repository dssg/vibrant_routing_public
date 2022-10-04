import time
import json
import logging
import joblib
import yaml

from config.project_constants import MODELING_CONFIG_FILE
from src.pipeline.call.matrix_creator import matrix_creator
from src.utils.pipeline_util import split_features_label
from src.utils.sql_util import get_db_conn, add_model_entry_to_db
from src.utils.util import create_hash

import importlib
import itertools

import click


def model_trainer(
    db_conn,
    matrix,
    model_type,
    model_params,
    split,
    label_column_name,
    model_folder_path,
    train_matrix_file_path,
    log_path,
    columns_to_remove=None,
    save_model=True,
):
    """Get trained model. This function returns the trained model and saves it
    in a pickled version of the object in binary format based on save_model.
    The name of the outputted pickled is a hashed value of the characteristic
    of the model and looks like this: "7b2000973a50608b3a184a67ee1900b6.pickle".

    It is important to note that this code was written to be compatible with the sklearn API,
    such that models have built-in fit() and predict_proba() methods.

    Keyword arguments:
        db_conn (object) -- database connection.
        matrix (pd.DataFrame) -- dataset containing both features and labels together.
        model_type (str) -- type of model to be trained
        model_params (dict) -- hyperparams to use in training
        split (dict) -- dictionary with the split information.
        label_column_name (str) -- name of label column in the dataset matrix.
        model_folder_path (str) -- folder path where to store the trained model.
        train_matrix_file_path (str) -- file path where the train matrix is stored for model governance.
        log_path (str) -- complete path where the logs are saved.
        columns_to_remove (list[str]) -- list of columns to remove from dataset matrix for training.
                                      Defaults to NoneType.
        save_model (bool) -- whether or not to save the trained model. Defaults to True.

    Returns:
        model_id (int) -- identifier of the entry created in the database for model governance.
        trained_model (object) -- model that was trained on the features and labels from the matrix.
    """
    # Get the feature and label splits.
    X, y = split_features_label(
        matrix=matrix,
        label_column_name=label_column_name,
        columns_to_remove=columns_to_remove,
    )

    # Initialize the model.
    # Example of what model_path and model_function should be:
    #     model_path = 'sklearn.linear_model'
    #     model_function = 'LogisticRegression'
    model_path, model_function = model_type.rsplit(".", 1)

    logging.debug(f"---\nmodel_path: {model_path} | model_function: {model_function}.")
    module = importlib.import_module(model_path)
    model = getattr(module, model_function)(**model_params)

    # Extract parent directory name from module path.
    # Example:
    # a = "src.pipeline.baseline._classes"
    # result = a.rsplit(".", 2)[1]
    # >>> result
    # "baseline"
    model_parent_dir = model.__module__.rsplit(".", 2)[1]

    if model_parent_dir == "baseline":
        # The baseline requires the datetime split for the matrix.
        kwargs = split

        # Joblib pickle is unable to save local class.
        # Error message:
        #        _pickle.PicklingError: Can't pickle <function create_engine.<locals>.connect at 0x7f8ded566830>:
        #        it's not found as sqlalchemy.engine.create.create_engine.<locals>.connect
        # Set save_model to False by default.
        save_model = False
    else:
        kwargs = {}

    start_time = time.time()
    # Train the model
    trained_model = model.fit(X, y, **kwargs)

    # Set the complete path where to store the model.
    model_hash = create_hash(
        dict_to_hash={
            "model_type": str(model_type),
            "model_params": str(model_params),
            "matrix_start_datetime": str(split["start_datetime_est"]),
            "matrix_end_datetime": str(split["end_datetime_est"]),
            "matrix_column_names": ",".join(matrix.columns),
            "matrix_rows": str(matrix.shape[0]),
            "columns_to_remove": ",".join(columns_to_remove),
            "label_column_name": ",".join([label_column_name]),
        }
    )
    model_file_path = f"{model_folder_path}{model_hash}.pickle"

    if save_model:
        # Save the trained model to disk: datetime_model_trained.pickle.
        joblib.dump(value=model, filename=model_file_path)

    # Add model entry to database for model governance.
    model_id = add_model_entry_to_db(
        db_conn=db_conn,
        model_class=type(model).__name__,
        pickle_path=model_file_path,
        parameters=json.dumps(model.get_params()).replace("NaN", "null"),
        label=label_column_name,
        features=X.columns.to_list(),
        split=split,
        train_matrix_path=train_matrix_file_path,
        log_path=log_path,
    )

    # Log training + save time.
    logging.info(f"Model:{model_id}, Train time: {time.time() - start_time} seconds.")

    return model_id, trained_model


def get_model_init(db_conn, model_grid_value):
    """Get the initial configuration for a given model.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_grid_value (dict) -- parameter values of the model grid.

    Returns:
        init_param (dict) -- initial parameters.
        model_grid_value (dict) -- updated parameter values of the model grid.
    """

    # Filter out the train_flag attribute that is used to check
    # whether or not to train the model group.
    if "train_flag" in model_grid_value.keys():
        model_grid_value = {
            k: v for k, v in model_grid_value.items() if k != "train_flag"
        }

    if "init_param" in model_grid_value.keys():
        init_param = model_grid_value["init_param"]
        if "db_conn" in init_param.keys():
            init_param["db_conn"] = db_conn

        model_grid_value = {
            k: v for k, v in model_grid_value.items() if k != "init_param"
        }
    else:
        init_param = {}

    return init_param, model_grid_value


def get_all_models_config(db_conn, model_grid):
    """Get configuration for all models set in model_grid.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_grid (dict]) -- types of the models to train and their hyperparameters.

    Returns:
        list_all_models_config (list) -- list with the configuration of all models specified in model_grid.
                                         The list consists of tuples such as (model_type, params).
    """
    # Declare empty list to store the configuration of all models.
    list_all_models_config = []

    # Loop over the model_grid to get all relevant information.
    for model_grid_key, model_grid_value in model_grid.items():

        # Compute the grid only if the train_flag attribute in the
        # model_grid config dictionary is set to True.
        if model_grid_value["train_flag"]:
            init_param, model_grid_value = get_model_init(
                db_conn=db_conn, model_grid_value=model_grid_value
            )

            if not model_grid_value:
                list_all_models_config.append((model_grid_key, init_param))
            else:
                # Unpack the key, value of param_possibilities
                param_possibilities = model_grid_value.copy()
                param_names_list = param_possibilities.keys()
                param_values_list = list(param_possibilities.values())

                # List to store the combinations of params.
                unique_param_combinations = list(itertools.product(*param_values_list))

                # Get unique combinations of the params
                # Note: https://stackoverflow.com/questions/3034014/how-to-apply-itertools-product-to-elements-of-a-list-of-lists
                for param_combination in unique_param_combinations:
                    # Dictionary to store the parameter settings of the current model.
                    model_grid_param = dict(zip(param_names_list, param_combination))

                    # Update the model_grid_param with init_param.
                    model_grid_param.update(init_param)

                    # Append model configuration list of models to later train.
                    list_all_models_config.append((model_grid_key, model_grid_param))

    return list_all_models_config


def all_models_trainer(
    db_conn,
    matrix,
    model_grid,
    split,
    label_column_name,
    model_folder_path,
    train_matrix_file_path,
    log_path,
    columns_to_remove,
    save_model=True,
):
    """Get all trained models. This function returns the trained models and save them
    in a pickled version of the object in binary format based on save_model.
    The name of the outputted pickled is a hashed value of the characteristic
    of the model and looks like this: "7b2000973a50608b3a184a67ee1900b6.pickle".

    Keyword arguments:
        db_conn (object) -- database connection.
        matrix (pd.DataFrame) -- dataset containing both features and labels together.
        model_grid (dict]) -- types of the models to train and their hyperparameters.
        split (dict) -- dictionary with the split information.
        label_column_name (str) -- name of label column in the dataset matrix.
        model_folder_path (str) -- folder path where to store the trained models.
        train_matrix_file_path (str) -- file path where the train matrix is stored for model governance.
        log_path (str) -- complete path where the logs are saved.
        columns_to_remove (list[str]) -- list of columns to remove from dataset matrix for training.
        save_model (bool) -- whether or not to save the trained model. Defaults to True.

    Returns:
        model_ids (list[int]) -- list of identifiers of the entry created in the database for model governance.
        trained_models (list[object]) -- list of models trained on the features and labels from the matrix.
    """
    model_ids = []
    trained_models = []

    list_all_models_config = get_all_models_config(
        db_conn=db_conn, model_grid=model_grid
    )

    # Train each of the given models
    for model_type, model_params in list_all_models_config:
        model_id, trained_model = model_trainer(
            db_conn=db_conn,
            matrix=matrix,
            model_type=model_type,
            model_params=model_params,
            split=split,
            label_column_name=label_column_name,
            model_folder_path=model_folder_path,
            train_matrix_file_path=train_matrix_file_path,
            log_path=log_path,
            columns_to_remove=columns_to_remove,
            save_model=save_model,
        )

        # Log that this model was trained.
        model_ids.append(model_id)
        trained_models.append(trained_model)

    return model_ids, trained_models


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

    train_matrix_file_path, train_matrix = matrix_creator(
        db_conn=db_conn,
        split=splits[-1]["train"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )

    log_path = ""

    model_grid = modeling_config["model_grid"]
    logging.debug(f"Model grid: {model_grid}.")

    model_ids, models = all_models_trainer(
        db_conn=db_conn,
        matrix=train_matrix,
        model_grid=modeling_config["model_grid"],
        split=splits[-1]["train"],
        label_column_name=modeling_config["matrix_creator_config"]["label_column_name"],
        model_folder_path=modeling_config["model_folder_path"],
        columns_to_remove=modeling_config["matrix_creator_config"]["columns_to_remove"],
        train_matrix_file_path=train_matrix_file_path,
        log_path=log_path,
    )


# main()
