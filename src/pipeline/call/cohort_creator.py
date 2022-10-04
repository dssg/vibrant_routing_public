import json
import yaml
import click

from config.project_constants import MODELING_CONFIG_FILE, ROLE_NAME
from src.utils.sql_util import (
    create_schema,
    create_table_with_sql_query,
    get_db_conn,
    set_role,
    create_index,
)


def cohort_creator(
    db_conn,
    database_config_dict,
    splits,
    cohort_config_dict,
    role_name=None,
):
    """Create cohort using datetime from <splits>.

    Keyword arguments:
        db_conn (object) -- database connection.
        database_config_dict (dict) -- dictionary with the database configuration parameters.
        splits (list) -- list of dictionaries with the time splits to create the train and validation datasets.
                         The time splits will be performed assuming inclusive intervals (e.g., train dataset = [train start, train end]).
        cohort_config_dict (dict) -- dictionary with the cohort configuration.
        role_name (str) -- role name. Defaults to NoneType.
    """
    # Set role if role_name is not None
    if role_name is not None:
        set_role(db_conn=db_conn, role_name=role_name)

    # Get ends of time splits: the start time will be
    start_datetime = min(splits, key=lambda x: x["train"]["start_datetime_est"])[
        "train"
    ]["start_datetime_est"]
    end_datetime = max(splits, key=lambda x: x["validation"]["end_datetime_est"])[
        "validation"
    ]["end_datetime_est"]

    # Create schema if doesn't already exist
    create_schema(
        db_conn=db_conn, schema_name=database_config_dict["modeling_schema_name"]
    )

    table_content = cohort_config_dict["query"].format(
        source_data_schema_name=database_config_dict["source_data_schema_name"],
        source_data_table_name=database_config_dict["source_data_table_name"],
        start_datetime_est=start_datetime,
        end_datetime_est=end_datetime,
    )

    create_table_with_sql_query(
        db_conn=db_conn,
        schema_name=database_config_dict["modeling_schema_name"],
        table_name=database_config_dict["cohort_table_name"],
        table_content=table_content,
    )

    for index in cohort_config_dict["indexes"]:
        create_index(
            db_conn=db_conn,
            schema_name=database_config_dict["modeling_schema_name"],
            table_name=database_config_dict["cohort_table_name"],
            column_name=index,
        )


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

    # Read json file containing split data dictionary.
    with open(splits, "r") as f:
        splits = json.load(f)

    cohort_creator(
        db_conn=get_db_conn(),
        database_config_dict=modeling_config["database_config"],
        splits=splits,
        cohort_config_dict=modeling_config["cohort_config"],
        role_name=ROLE_NAME,
    )


# main()
