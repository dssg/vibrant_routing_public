import gc
import logging
import time
import yaml
import json
import click

from config.project_constants import MODELING_CONFIG_FILE, ROLE_NAME

from src.pipeline.call import (
    cohort_creator,
    evaluate,
    feature_creator,
    label_creator,
    matrix_creator,
    get_all_models_config,
    model_trainer,
    predict,
    split_data,
)
from src.utils.logging_util import set_logging_configuration
from src.utils.pipeline_util import (
    split_features_label,
    set_prefix_name_of_plot,
    feature_importance,
)
from src.utils.sql_util import (
    get_db_conn,
    get_experiment_id,
)
from src.utils.plot_util import (
    plot_scores_vs_actual_labels,
)


@click.command()
@click.option(
    "--create_cohort",
    prompt="Create cohort table?",
    default=False,
)
@click.option(
    "--create_label",
    prompt="Create label table?",
    default=False,
)
@click.option(
    "--create_features",
    prompt="Create features tables?",
    default=False,
)
@click.option(
    "--create_plots",
    prompt="Create plots?",
    default=True,
)
def run(
    create_cohort,
    create_label,
    create_features,
    create_plots,
):
    """Function that runs the call-level pipeline.

    Keyword arguments:
        create_cohort (bool) -- indicates whether the cohort table should be created.
        create_label (bool) -- indicates whether the label table should be created.
        create_features (bool) -- indicates whether the features tables should be created.
        create_plots (bool) -- indicates whether plots should be created.
    """
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Set logging configuration and return the complete path where the logs are saved.
    log_path = set_logging_configuration(
        log_folder_path=modeling_config["log_folder_path"]
    )
    logging.info("Execution of the pipeline started.")

    # Log content of yaml file.
    logging.debug(
        f"This is the configuration file for this log: "
        f"json.dumps(modeling_config, indent=4)"
    )
    logging.debug(json.dumps(modeling_config, indent=4))

    # Get database connection.
    db_conn = get_db_conn()

    # Create time splits.
    logging.info("Creation of time splits started.")
    splits = split_data(temporal_config=modeling_config["temporal_config"])
    logging.info("Creation of time splits finished.")

    # Check whether the cohort, label, and features tables should be created.
    if create_cohort:
        # Create cohort.
        logging.info("Creation of cohort started.")
        cohort_creator(
            db_conn=db_conn,
            database_config_dict=modeling_config["database_config"],
            splits=splits,
            cohort_config_dict=modeling_config["cohort_config"],
            role_name=ROLE_NAME,
        )
        logging.info("Creation of cohort finished.")
    else:
        logging.info("Creation of cohort skipped as stated in create_cohort.")

    if create_label:
        # Create label for the cohort of interest.
        logging.info("Creation of label started.")
        label_creator(
            db_conn=db_conn,
            database_config_dict=modeling_config["database_config"],
            query=modeling_config["label_config"]["query"],
            role_name=ROLE_NAME,
        )
        logging.info("Creation of label finished.")
    else:
        logging.info("Creation of label skipped as stated in create_label.")

    if create_features:
        # Create features for the cohort of interest.
        logging.info("Creation of features started.")
        start_time = time.time()
        feature_creator(
            db_conn=db_conn,
            source_data_schema_name=modeling_config["database_config"][
                "source_data_schema_name"
            ],
            source_data_table_name=modeling_config["database_config"][
                "source_data_table_name"
            ],
            modeling_schema_name=modeling_config["database_config"][
                "modeling_schema_name"
            ],
            cohort_table_name=modeling_config["database_config"]["cohort_table_name"],
            feature_schema_name=modeling_config["database_config"][
                "feature_schema_name"
            ],
            feature_config_dict=modeling_config["feature_config"],
        )
        logging.info(
            f"Creation of features finished. Execution time: {time.time() - start_time} seconds."
        )
    else:
        logging.info("Creation of features skipped as stated in create_features.")

    # Get configuration for all models specified in the model grid.
    list_all_models_config = get_all_models_config(
        db_conn=db_conn, model_grid=modeling_config["model_grid"]
    )
    logging.info(
        f"We are going to train and evaluate {len(list_all_models_config)} models."
    )

    # Train a model and evaluate it for each split created.
    logging.info(f"Loop per data split started. There are {len(splits)} splits.")
    for index, split in enumerate(splits):
        logging.info(f"Pipeline started for split #{index}.")
        # Get train matrix.
        logging.info(
            f"Creation of train matrix started."
            f"Time range from {split['train']['start_datetime_est']} to {split['train']['end_datetime_est']}."
        )
        train_matrix_file_path, train_matrix = matrix_creator(
            db_conn=db_conn,
            split=split["train"],
            schema_name=modeling_config["database_config"]["modeling_schema_name"],
            database_config_dict=modeling_config["database_config"],
            feature_config_dict=modeling_config["feature_config"],
            matrix_config_dict=modeling_config["matrix_creator_config"],
            matrix_folder_path=modeling_config["matrix_folder_path"],
        )
        logging.info("Creation of train matrix finished.")

        # Get validation matrix.
        logging.info(
            f"Creation of validation matrix started."
            f"Time range from {split['validation']['start_datetime_est']} to {split['validation']['end_datetime_est']}."
        )
        validation_matrix_file_path, validation_matrix = matrix_creator(
            db_conn=db_conn,
            split=split["validation"],
            schema_name=modeling_config["database_config"]["modeling_schema_name"],
            database_config_dict=modeling_config["database_config"],
            feature_config_dict=modeling_config["feature_config"],
            matrix_config_dict=modeling_config["matrix_creator_config"],
            matrix_folder_path=modeling_config["matrix_folder_path"],
        )
        # Get validation label from matrix.
        validation_features, validation_label = split_features_label(
            matrix=validation_matrix,
            label_column_name=modeling_config["matrix_creator_config"][
                "label_column_name"
            ],
            columns_to_remove=modeling_config["matrix_creator_config"][
                "columns_to_remove"
            ],
        )
        logging.info("Creation of validation matrix finished.")

        # Loop through the model grid to train all models.
        logging.info("Model trainer started.")
        for model_type, model_params in list_all_models_config:
            logging.info(
                f"Model trainer started for model_type:{model_type} and model_params:{model_params}"
            )
            model_id, model = model_trainer(
                db_conn=db_conn,
                matrix=train_matrix,
                model_type=model_type,
                model_params=model_params,
                split=split["train"],
                label_column_name=modeling_config["matrix_creator_config"][
                    "label_column_name"
                ],
                model_folder_path=modeling_config["model_folder_path"],
                train_matrix_file_path=train_matrix_file_path,
                log_path=log_path,
                columns_to_remove=modeling_config["matrix_creator_config"][
                    "columns_to_remove"
                ],
            )
            logging.info(f"Model with model_id:{model_id} finished training.")

            # Get feature importance.
            logging.info(
                f"Feature importance started to be calculated for model_id:{model_id}."
            )
            feature_importance(
                model=model,
                model_id=model_id,
                column_names=validation_features.columns.to_list(),
            )
            logging.info(
                f"Feature importance finished to be calculated for model_id:{model_id}."
            )

            # Predict on validation matrix.
            logging.info(f"Prediction started for model_id:{model_id}.")
            y_predicted = predict(
                db_conn=db_conn,
                matrix=validation_matrix,
                matrix_file_path=validation_matrix_file_path,
                model=model,
                model_id=model_id,
                split=split["validation"],
                label_column_name=modeling_config["matrix_creator_config"][
                    "label_column_name"
                ],
                columns_to_remove=modeling_config["matrix_creator_config"][
                    "columns_to_remove"
                ],
                pos_label=1,
            )
            logging.info(f"Prediction finished for model_id:{model_id}.")

            # Evaluate the trained model.
            logging.info(f"Evaluation started for model_id:{model_id}.")
            metrics = evaluate(
                db_conn=db_conn,
                model_id=model_id,
                split=split["validation"],
                y_true=validation_label,
                y_predicted=y_predicted,
                metrics_to_evaluate=modeling_config["scoring"],
                validation_matrix_file_path=validation_matrix_file_path,
            )
            logging.info(
                f"Evaluation finished for model_id:{model_id}."
                f"{type(model).__name__} AUC-ROC: {metrics['auc-roc']}"
            )

            if create_plots:
                # Plot the results to facilitate its interpretation.
                logging.info("Ploting started.")
                # Set the prefix of the plot filenames.
                experiment_id = get_experiment_id(
                    db_conn=db_conn,
                    model_id=model_id,
                    split=split["validation"],
                    evaluation_matrix_path=validation_matrix_file_path,
                )
                plot_prefix_filename = set_prefix_name_of_plot(
                    plots_folder_path=modeling_config["plots_folder_path"],
                    model_id=model_id,
                    experiment_id=experiment_id,
                )
                # Call the plot functions.
                plot_scores_vs_actual_labels(
                    y_true=validation_label,
                    y_predicted=y_predicted,
                    plot_prefix_filename=plot_prefix_filename,
                )
                logging.info("Ploting finished.")
                del experiment_id, plot_prefix_filename
            else:
                logging.info("Plots skipped as indicated in create_plots.")
            logging.info(
                f"Model trainer finished for model_type:{model_type} and model_id:{model_id}."
            )
            del (y_predicted, metrics, model, model_id, model_type, model_params)
        del (
            train_matrix_file_path,
            train_matrix,
            validation_matrix_file_path,
            validation_matrix,
            validation_features,
            validation_label,
        )
        gc.collect()
        logging.info(f"Pipeline finished for split #{index}.")

    logging.info("Pipeline execution finished.")
