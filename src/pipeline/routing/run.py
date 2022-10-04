import logging
import yaml
import json
import click
import joblib

from config.project_constants import MODELING_CONFIG_FILE
from src.pipeline.routing import (
    split_data,
    cohort_creator,
    simulate_routing,
    evaluate_routing,
)
from src.utils.logging_util import set_logging_configuration
from src.utils.sql_util import (
    get_db_conn,
    add_routing_evaluation_entry_to_db,
)


@click.command()
@click.option(
    "--routing_table_path",
    prompt="Path to routing table to be tested.",
    default="/mnt/data/projects/vibrant-routing/data/20220604/vibrant_RoutingTable_202206031725.csv",
)
@click.option(
    "--number_of_trials",
    prompt="How many times to re-run each experiment.",
    default=3,
)
def run(routing_table_path, number_of_trials):
    """Function that runs the routing-level pipeline.

    Keyword arguments:
        routing_table_path (str) -- Path to routing table to be tested.
        number_of_trials (int) -- Number of time to re-run the experiment.
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

    routing_level_config = modeling_config["routing_level_config"]
    # Create time splits.
    logging.info("Creation of time splits started.")
    split_datetime = split_data(
        temporal_config=routing_level_config["temporal_config"],
        output_filename=None,
    )
    logging.info("Creation of time splits finished.")

    # Retrieve the best model_path from config.
    best_model_path = routing_level_config["best_model_config"]["model_pickle_path"]
    logging.info(f"Path to best model: {best_model_path}")

    # Load the best model from from model_path.
    logging.info("Loading of best model started.")
    model = joblib.load(best_model_path)
    logging.info("Loading of best model finished.")

    # Create cohort.
    logging.info(
        "Creation of cohort and lookup tables needed because simulation started."
    )
    cohort_creator(
        db_conn=get_db_conn(),
        split_datetime=split_datetime,
        config=routing_level_config,
    )
    logging.info("Creation of cohort and lookup tables needed for simulation finished.")

    for trial_number in range(number_of_trials):
        # Recreate the simulation table by setting off the table_flag
        # for the other tables.
        tables_to_create = routing_level_config["tables_to_create"]
        for i in range(len(tables_to_create)):
            if tables_to_create[i]["name"] != "simulated_routing_attempts":
                tables_to_create[i]["table_flag"] = False
            else:
                tables_to_create[i]["table_flag"] = True

        # Overwrite the tables_to_create config parameters.
        routing_level_config["tables_to_create"] = tables_to_create

        logging.info("Creation of simulated routing attempt tables.")
        cohort_creator(
            db_conn=get_db_conn(),
            split_datetime=split_datetime,
            config=routing_level_config,
        )
        logging.info(
            "Creation of cohort and lookup tables needed for simulation finished."
        )

        # Simulate the routing of calls.
        logging.info(
            f"Call simulation started. # {trial_number+1}/{number_of_trials} re-runs."
        )
        random_seed = simulate_routing(
            db_conn=db_conn,
            model=model,
            routing_table_path=routing_table_path,
            config_routing_level=routing_level_config,
            config_feature=modeling_config["feature_config"],
            random_seed=None,
        )
        logging.info("Call simulation finished.")
        logging.info("Adding routing evaluation entry to db.")
        evaluation_id = add_routing_evaluation_entry_to_db(
            db_conn=db_conn,
            model_path=best_model_path,
            trial_number=trial_number + 1,
            routing_table_path=routing_table_path,
            config_routing=routing_level_config,
            config_feature=modeling_config["feature_config"],
            random_seed=random_seed,
            log_path=log_path,
        )
        logging.info("Finished adding routing evaluation entry to db.")
        logging.info(
            f"Evaluation started: \npath to best model:{best_model_path} \npath to routing table: {routing_table_path}"
        )
        evaluate_routing(db_conn=db_conn, evaluation_id=evaluation_id)
        logging.info(
            f"Evaluation finished: \npath to best model:{best_model_path} \npath to routing table: {routing_table_path}"
        )
        logging.info(
            f"Call simulation finished. # {trial_number+1}/{number_of_trials} re-runs."
        )
    logging.info("Pipeline execution finished.")
