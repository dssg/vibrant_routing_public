import time
import logging
from sqlalchemy import null
import yaml

from config.project_constants import (
    MODELING_CONFIG_FILE,
    ROLE_NAME,
    FEATURES_TABLE_INDEX,
)

from src.utils.sql_util import (
    create_table_with_sql_query,
    get_db_conn,
    set_role,
    create_schema,
    create_index,
)

from src.utils.pipeline_util import complete_query_fillings_for_skeleton

from config.project_constants import (
    ROLE_NAME,
    MODELING_CONFIG_FILE,
    PRE_COMPUTED_FEATURES_SCHEMA_NAME,
    PRE_COMPUTED_FEATURES_TABLE_NAME,
    PRE_COMPUTED_FEATURES_WITH_COHORT_TABLE_NAME,
)


def feature_creator(
    db_conn,
    source_data_schema_name,
    source_data_table_name,
    modeling_schema_name,
    cohort_table_name,
    feature_schema_name,
    feature_config_dict,
):
    """Create features at the call level.

    Keyword arguments:
        db_conn (object) -- database connection.
        source_data_schema_name (str) -- schema name of the source data.
        source_data_table_name (str) -- table name of the source data.
        modeling_schema_name (str) -- schema name of the split data.
        cohort_table_name (str) -- name of the cohort table.
        feature_schema_name (str) -- name of the feature schema.
        feature_config_dict (dict) -- information about the features to be created.
    """

    # Set role to role_name.
    if ROLE_NAME is not None:
        set_role(db_conn=db_conn, role_name=ROLE_NAME)

    # Create a schema to store the feature tables
    create_schema(db_conn=db_conn, schema_name=feature_schema_name)

    # Create pre_computed_features table under "features" schema by joining with the cohort
    pre_computed_features_skeleton = feature_config_dict[
        "pre_computed_features_skeleton"
    ]
    pre_computed_features_query = pre_computed_features_skeleton.format(
        modeling_schema_name=modeling_schema_name,
        cohort_table_name=cohort_table_name,
        pre_computed_features_schema_name=PRE_COMPUTED_FEATURES_SCHEMA_NAME,
        pre_computed_features_table_name=PRE_COMPUTED_FEATURES_TABLE_NAME,
    )

    create_table_with_sql_query(
        db_conn=db_conn,
        schema_name=feature_schema_name,
        table_name=PRE_COMPUTED_FEATURES_WITH_COHORT_TABLE_NAME,
        table_content=pre_computed_features_query,
    )

    create_index(
        db_conn=db_conn,
        schema_name=feature_schema_name,
        table_name=PRE_COMPUTED_FEATURES_WITH_COHORT_TABLE_NAME,
        column_name=FEATURES_TABLE_INDEX,
    )

    # Generate the query skeleton to begin creating dynamically computed features.
    query_skeleton = feature_config_dict["query_skeleton"]

    # Take one feature family at a time (e.g. NUMBER_OF_ROUTING_ATTEMPTS and then NUMBER_OF_CALLS_AT_CENTER_PAST).
    for feature_family, feature_family_values in feature_config_dict[
        "query_fillings"
    ].items():
        logging.debug(f"\tCreating features table for {feature_family}.")
        # Generate complete query filling.
        complete_query_filling = complete_query_fillings_for_skeleton(
            feature_family_values
        )

        # Insert the generated whole_query into the skeleton from above.
        whole_query = query_skeleton.format(
            source_data_schema_name=source_data_schema_name,
            source_data_table_name=source_data_table_name,
            modeling_schema_name=modeling_schema_name,
            cohort_table_name=cohort_table_name,
            query_filling=complete_query_filling,
        )

        create_table_with_sql_query(
            db_conn=db_conn,
            schema_name=feature_schema_name,
            table_name=feature_family,
            table_content=whole_query,
        )

        create_index(
            db_conn=db_conn,
            schema_name=feature_schema_name,
            table_name=feature_family,
            column_name=FEATURES_TABLE_INDEX,
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
    # Testing configuration for call-level feature creation.
    feature_creator(
        db_conn=get_db_conn(),
        source_data_schema_name=modeling_config["database_config"][
            "source_data_schema_name"
        ],
        source_data_table_name=modeling_config["database_config"][
            "source_data_table_name"
        ],
        modeling_schema_name=modeling_config["database_config"]["modeling_schema_name"],
        cohort_table_name=modeling_config["database_config"]["cohort_table_name"],
        feature_schema_name=modeling_config["database_config"]["feature_schema_name"],
        feature_config_dict=modeling_config["feature_config"],
    )

    # End the timer for logging.
    end_time = time.time() - start_time
    logging.debug(f"Feature Creation completed at {end_time}.")


# main()
