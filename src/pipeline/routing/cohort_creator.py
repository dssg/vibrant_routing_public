import json
import logging

import yaml
from config.project_constants import (
    ROLE_NAME,
    ROUTING_LEVEL_SCHEMA_NAME,
    SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
    SOURCE_DATA_SCHEMA_NAME,
    MODELING_CONFIG_FILE,
    ROUTING_LEVEL_SPLIT_FILE,
)
from src.utils.sql_util import (
    create_index,
    create_schema,
    create_table_with_sql_query,
    get_db_conn,
    set_role,
)


def cohort_creator(
    db_conn,
    config,
    split_datetime,
):
    """Create active, historical, and simulated calls cohort table using datetime from <splits>.
    The simulated calls cohort is an empty table.

    Keyword arguments:
        db_conn (object) -- database connection.
        config (dict) -- dictionary with <routing_level> configuration.
        split_datetime (dict) -- dictionary with time splits to create the
                                 active call queue and historical datasets.

    Raises:
        NotImplementedError -- if the value of `tag` parameter from `config[<table_name>]['tag']` is None.
    """

    # Set role if role_name is not None.
    if ROLE_NAME is not None:
        set_role(db_conn=db_conn, role_name=ROLE_NAME)

    # Create schema if it doesn't already exist.
    create_schema(db_conn=db_conn, schema_name=ROUTING_LEVEL_SCHEMA_NAME)

    # Loop through tables in the config dictionary.
    for table in config["tables_to_create"]:
        table_name = table["name"]
        table_query = table["query"]
        table_indexes = table["indexes"]
        table_tag = table["tag"]
        table_flag = table["table_flag"]

        if not table_flag:
            logging.info(
                f"{table_name} table creation is skipped because table_flag == False."
            )
            continue

        logging.info(f"Creating {table_name} table...")
        if table_tag == "simulated":
            table_content = table_query.format(
                source_data_schema_name=SOURCE_DATA_SCHEMA_NAME,
                source_data_table_name=SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
            )
        elif table_tag == "historical":
            table_content = table_query.format(
                source_data_schema_name=SOURCE_DATA_SCHEMA_NAME,
                source_data_table_name=SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
                start_datetime_est=split_datetime["historical"]["start_datetime_est"],
                end_datetime_est=split_datetime["historical"]["end_datetime_est"],
            )
        elif table_tag == "future":
            table_content = table_query.format(
                source_data_schema_name=SOURCE_DATA_SCHEMA_NAME,
                source_data_table_name=SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
                start_datetime_est=split_datetime["simulation"]["start_datetime_est"],
                end_datetime_est=split_datetime["simulation"]["end_datetime_est"],
            )
        else:
            logging.error(f"Unrecognized tag: {table_name} table has tag={table_tag}.")
            raise NotImplementedError(
                f"Unrecognized tag: {table_name} table has tag={table_tag}."
            )

        # Create table.
        create_table_with_sql_query(
            db_conn=db_conn,
            schema_name=ROUTING_LEVEL_SCHEMA_NAME,
            table_name=table_name,
            table_content=table_content,
        )

        # Create table indexes.
        for index in table_indexes:
            create_index(
                db_conn=db_conn,
                schema_name=ROUTING_LEVEL_SCHEMA_NAME,
                table_name=table_name,
                column_name=index,
            )


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Read json file containing dictionary with historical and simulation datetime.
    with open(ROUTING_LEVEL_SPLIT_FILE, "r") as f:
        split_datetime = json.load(f)

    cohort_creator(
        db_conn=get_db_conn(),
        split_datetime=split_datetime,
        config=modeling_config["routing_level_config"],
    )


# main()
