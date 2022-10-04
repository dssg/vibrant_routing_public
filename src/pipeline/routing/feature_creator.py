import time
import logging
import yaml

from config.project_constants import MODELING_CONFIG_FILE, ROLE_NAME

from src.utils.sql_util import (
    create_table_with_sql_query,
    get_db_conn,
    set_role,
    create_schema,
)
from src.utils.pipeline_util import complete_query_fillings_for_skeleton


def feature_creator(
    db_conn,
    source_data_schema_name,
    source_data_table_name,
    feature_schema_name,
    feature_config_dict,
    incoming_call_dict,
    simulated_routing_attempts_table_name,
):
    """Create features at the routing level.

    Keyword arguments:
        db_conn (object) -- database connection.
        source_data_schema_name (str) -- schema name of the source data.
        source_data_table_name (str) -- table name of the source data.
        feature_schema_name (str) -- name of the feature schema.
        feature_config_dict (dict) -- information about the features to be created.
        incoming_call_dict (dict) -- a dictionary containing information about the incoming call:
                                    Example of incoming_call_dict:
                                        incoming_call_dict = {
                                            "call_key": "e4011-20220526002924403-500",
                                            "arrived_datetime_est": "2022-05-26 00:29:24.000",
                                            "center_key": "IL460000",
                                            "termination_number": 6304823616,
                                        }.
        simulated_routing_attempts_table_name (str) -- table name of simulated_routing_attempts data.
    """

    # Set role to role_name.
    if ROLE_NAME is not None:
        set_role(db_conn=db_conn, role_name=ROLE_NAME)

    # Create a schema to store the feature tables
    create_schema(db_conn=db_conn, schema_name=feature_schema_name)

    # Generate the query skeleton for regualr features.
    query_skeleton = feature_config_dict["query_skeleton_routing_level"]

    # Take one feature family at a time (e.g. NUMBER_OF_ROUTING_ATTEMPTS and then NUMBER_OF_FLOW_OUTS).
    for feature_family, feature_family_values in feature_config_dict[
        "query_fillings"
    ].items():
        # Generate complete query filling.
        complete_query_filling = complete_query_fillings_for_skeleton(
            feature_family_values
        )

        # Insert the generated whole_query into the skeleton from above.
        whole_query = query_skeleton.format(
            source_data_schema_name=source_data_schema_name,
            source_data_table_name=source_data_table_name,
            query_filling=complete_query_filling,
            call_key=incoming_call_dict["call_key"],
            center_key=incoming_call_dict["center_key"],
            termination_number=incoming_call_dict["termination_number"],
            arrived_datetime_est=incoming_call_dict["arrived_datetime_est"],
            simulated_routing_attempts_table_name=simulated_routing_attempts_table_name,
        )

        create_table_with_sql_query(
            db_conn=db_conn,
            schema_name=feature_schema_name,
            table_name=feature_family,
            table_content=whole_query,
        )

    # Generate the query skeleton for special features that requires a different skeleton.
    query_skeleton_augment = feature_config_dict["query_skeleton_routing_level_augment"]

    # Take one feature family at a time.
    for feature_family, feature_family_values in feature_config_dict[
        "query_fillings_augment"
    ].items():
        # Generate complete query filling.
        complete_query_filling = complete_query_fillings_for_skeleton(
            feature_family_values
        )

        # Insert the generated whole_query into the skeleton from above.
        whole_query = query_skeleton_augment.format(
            source_data_schema_name=source_data_schema_name,
            source_data_table_name=source_data_table_name,
            query_filling=complete_query_filling,
            call_key=incoming_call_dict["call_key"],
            center_key=incoming_call_dict["center_key"],
            termination_number=incoming_call_dict["termination_number"],
            arrived_datetime_est=incoming_call_dict["arrived_datetime_est"],
            simulated_routing_attempts_table_name=simulated_routing_attempts_table_name,
        )

        create_table_with_sql_query(
            db_conn=db_conn,
            schema_name=feature_schema_name,
            table_name=feature_family,
            table_content=whole_query,
        )


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Start the timer for logging.
    start_time = time.time()
    logging.debug(f"Feature Creation started at {start_time}.")

    # Generate the features based on the given cohort.
    # Testing configuration for routing-level feature creation
    incoming_call_dict = {
        "call_key": "e4355-20220423224029741-450",  # "e4414-20211214210906628-532"
        "center_key": "CA123530",
        "termination_number": 5302315514,
        "arrived_datetime_est": "2022-04-23 22:41:08.000",  # "2021-12-14 21:09:53.000"
    }
    feature_creator(
        db_conn=get_db_conn(),
        source_data_schema_name=modeling_config["routing_level_config"][
            "database_config"
        ]["schema_name"],
        source_data_table_name=modeling_config["routing_level_config"][
            "feature_config"
        ]["source_data_table_name"],
        feature_schema_name=modeling_config["routing_level_config"]["feature_config"][
            "feature_schema_name"
        ],
        feature_config_dict=modeling_config["feature_config"],
        incoming_call_dict=incoming_call_dict,
        simulated_routing_attempts_table_name=modeling_config["routing_level_config"][
            "feature_config"
        ]["simulated_routing_attempts_table_name"],
    )

    # End the timer for logging.
    end_time = time.time() - start_time
    logging.debug(f"Feature Creation completed at {end_time}.")


# main()
