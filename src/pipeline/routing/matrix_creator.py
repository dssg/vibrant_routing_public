import logging
import pandas as pd
import yaml

from src.utils.sql_util import get_db_conn
from config.project_constants import MODELING_CONFIG_FILE


def matrix_creator(
    db_conn,
    schema_name,
    database_config_dict,
    feature_config_dict,
    matrix_config_dict,
):
    """Read features table from the database.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
        database_config_dict (str) -- information about the database config.
        feature_config_dict (dict) -- information about the features to be created.
        matrix_config_dict (dict) -- information about the matrix to be created.

    Returns:
        matrix (pd.DataFrame) -- dataset containing features.
    """

    query_skeleton = matrix_config_dict["query_skeleton"]
    base_query_filling = matrix_config_dict["query_filling"]

    # Initialize the query filling.
    complete_query_filling = ""

    # Append one feature family at a time to the query.
    for feature_family, _ in feature_config_dict["query_fillings"].items():
        complete_query_filling += base_query_filling.format(
            feature_table_name=feature_family
        )

    # Append one feature family (pre-computed at call level, but dynamically computed at routing level) at a time to the query.
    for feature_family, _ in feature_config_dict["query_fillings_augment"].items():
        complete_query_filling += base_query_filling.format(
            feature_table_name=feature_family
        )

    # Insert the generated whole_query into the skeleton from above.
    whole_query = query_skeleton.format(
        schema_name=schema_name,
        cohort_table_name=database_config_dict["cohort_table_name"],
        query_filling=complete_query_filling,
    )

    logging.debug(f"This is the query:\n{whole_query}")

    matrix = pd.read_sql_query(whole_query, db_conn)
    logging.debug(f"The resulting matrix has shape:{matrix.shape}.")

    return matrix


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Testing for routing-level pipeline.
    matrix = matrix_creator(
        db_conn=get_db_conn(),
        schema_name=modeling_config["routing_level_config"]["database_config"][
            "schema_name"
        ],
        database_config_dict=modeling_config["routing_level_config"]["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["routing_level_config"][
            "matrix_creator_config"
        ],
    )

    logging.debug(f"The shape of the matrix created is {matrix.shape}.")


# main()
