import yaml
import logging
from src.utils.sql_util import (
    get_db_conn,
    set_role,
    create_schema,
    create_index,
    create_table_with_sql_query,
)
from src.utils.logging_util import set_logging_configuration
from config.project_constants import PRE_COMPUTED_FEATURES_CONFIG_FILE, ROLE_NAME


def pre_compute_features(db_conn, config):
    """Pre-compute expensive features for every routing attempt before the pipeline starts.

    Keyword arguments:
        db_conn (object) -- database connection.
        config (dict) -- dictionary with pre_computed_features configuration.
    """
    # Set role to "vibrant-routing-role"
    if ROLE_NAME is not None:
        set_role(db_conn, role_name=ROLE_NAME)

    schema_name = config["schema_name"]
    table_name = config["table_name"]
    index_column = config["index_column"]

    # Create pre_computed_features schema
    create_schema(db_conn, schema_name)

    query_skeleton = config["query_skeleton"]

    feature_families = config["query_fillings"]

    # Generate the whole query for that family.
    whole_query = ""

    for feature_family, feature_family_values in config["query_fillings"].items():
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

        # Append one feature at a time to the query.
        # If this feature has no parameters to buid upon
        if not parameter_1:
            logging.debug(f"Creating feature table with zero params.")
            whole_query += f"{base_query_filling},"

            if parameter_2 or parameter_3:
                logging.warning(
                    f"parameter_1 list is empty but parameter_2 or parameter_3 list is not: parameter_2={parameter_2}, parameter_3={parameter_3}. Features will not be built on parameter_2 or parameter_3."
                )
        else:
            # If this feature has only one param (i.e. parameter_1) to build upon
            if not parameter_2:
                logging.debug(f"Creating feature table with one param: {parameter_1}")
                for p_1 in parameter_1:
                    whole_query += f"{base_query_filling.format(parameter_1=p_1)},"
            # If this feature has two params (i.e. parameter_1 and parameter_2) to build upon:
            elif not parameter_3:
                logging.debug(
                    f"Creating feature table with two params: {parameter_1} and {parameter_2}"
                )
                for p_2 in parameter_2:
                    for p_1 in parameter_1:
                        whole_query += f"{base_query_filling.format(parameter_1=p_1, parameter_2=p_2)},"
            # If this feature has three params (i.e. parameter_1 and parameter_2 and parameter_3) to build upon:
            else:
                logging.debug(
                    f"Creating feature table with three params: {parameter_1} and {parameter_2} and {parameter_3}"
                )
                for p3 in parameter_3:
                    for p_2 in parameter_2:
                        for p_1 in parameter_1:
                            whole_query += f"{base_query_filling.format(parameter_1=p_1, parameter_2=p_2, parameter_3=p3)},"

    # Splice off the last trailing comma and extra newline.
    whole_query = whole_query.strip(",").strip()

    whole_query = query_skeleton.format(
        query_filling=whole_query,
    )

    # Create pre_computed_features table
    create_table_with_sql_query(
        db_conn=db_conn,
        schema_name=schema_name,
        table_name=table_name,
        table_content=whole_query,
    )

    create_index(db_conn, schema_name, table_name, index_column)


def main():

    is_user_sure = input(
        "Final check: Are you ABSOLUTELY SURE you want to run pre_computed_features\n\tand DROP THE TABLE that took 6 hours to create?\n Type 'True' or 'False': "
    )
    print(f"Your response is: {is_user_sure}.")

    if is_user_sure == "True":
        print("Running pre_computed_features.")

        db_conn = get_db_conn()

        # Read yaml file containing database configuration for modeling.
        with open(PRE_COMPUTED_FEATURES_CONFIG_FILE) as f:
            pre_computed_features_config = yaml.load(f, Loader=yaml.FullLoader)

        # Set logging configuration.
        set_logging_configuration(
            log_folder_path=pre_computed_features_config["log_folder_path"]
        )
        logging.info("Execution of the pipeline started.")

        # Start the timer for logging.
        logging.debug("Pre-computed feature creation started.")

        pre_compute_features(
            db_conn=db_conn,
            config=pre_computed_features_config,
        )

        # End the timer for logging.
        logging.debug(f"Pre-computed feature creation completed.")

    elif is_user_sure == "False":
        print("Okay. Will not run pre_computed_features.")

    else:
        print(
            "Invalid input. Will not run pre_computed_features. Valid inputs are 'True' or 'False'."
        )


main()
