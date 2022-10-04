from datetime import datetime
import logging
import json
import pandas as pd
import yaml
import click

from src.utils.sql_util import get_db_conn
from src.utils.util import create_hash
from config.project_constants import MODELING_CONFIG_FILE


def matrix_creator(
    db_conn,
    schema_name,
    database_config_dict,
    feature_config_dict,
    matrix_config_dict,
    matrix_folder_path,
    split,
):
    """Read features table from the database. In case split = None, it will be assumed that
    we are interested in the features for the routing-level. For that case, only the matrix is output.
    For the call-level pipeline the matrix will be saved in disk with a hashed named that characterizes it.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
        database_config_dict (str) -- information about the database config.
        feature_config_dict (dict) -- information about the features to be created.
        matrix_config_dict (dict) -- information about the matrix to be created.
        matrix_folder_path (str) -- folder path where to store the created matrix.
        split (dict) -- dictionary with the split ends datetime.

    Returns:
        matrix_file_path (str) -- file path where the matrix is stored for model governance.
                                  Only returned if this script is executed from the call-level pipeline.
        matrix (pd.DataFrame) -- dataset containing both features and labels together.
    """

    query_skeleton = matrix_config_dict["query_skeleton"]
    base_query_filling = matrix_config_dict["query_filling"]

    # Initialize the query filling.
    complete_query_filling = ""

    # Append one feature family at a time to the query.
    for feature_family, feature_family_values in feature_config_dict[
        "query_fillings"
    ].items():
        complete_query_filling += base_query_filling.format(
            feature_table_name=feature_family
        )
    logging.debug(f"This is the complete_query_filling:\n{complete_query_filling}")

    # Insert the generated whole_query into the skeleton from above.
    whole_query = query_skeleton.format(
        schema_name=schema_name,
        cohort_table_name=database_config_dict["cohort_table_name"],
        label_table_name=database_config_dict["label_table_name"],
        query_filling=complete_query_filling,
        split_start_datetime=split["start_datetime_est"],
        split_end_datetime=split["end_datetime_est"],
    )

    logging.debug(f"This is the query:\n{whole_query}")

    matrix = pd.read_sql_query(whole_query, db_conn)
    logging.debug(f"The resulting matrix has shape:{matrix.shape}.")

    # Store matrix in disk for traceability.
    matrix_hash = create_hash(
        dict_to_hash={
            "matrix_start_datetime": str(split["start_datetime_est"]),
            "matrix_end_datetime": str(split["end_datetime_est"]),
            "matrix_column_names": ",".join(matrix.columns),
            "matrix_column_logic": str(feature_config_dict["query_fillings"]),
            "matrix_rows": str(matrix.shape[0]),
            "columns_to_remove": ",".join(matrix_config_dict["columns_to_remove"]),
            "label_column_name": ",".join(matrix_config_dict["label_column_name"]),
        }
    )
    matrix_file_path = f"{matrix_folder_path}{matrix_hash}.csv"
    matrix.to_csv(matrix_file_path)
    return matrix_file_path, matrix


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

    # Testing for call-level pipeline.
    # For testing purposes: read splits.json.
    with open(splits) as f:
        splits = json.load(f)

    matrix_file_path, matrix = matrix_creator(
        db_conn=get_db_conn(),
        split=splits[0]["validation"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )

    logging.debug(f"The shape of the matrix created is {matrix.shape}.")


# main()
