import yaml

from config.project_constants import MODELING_CONFIG_FILE, ROLE_NAME, LABELS_TABLE_INDEX
from src.utils.sql_util import (
    create_index,
    get_db_conn,
    set_role,
    create_table_with_sql_query,
)


def label_creator(
    db_conn,
    database_config_dict,
    query,
    role_name=None,
):
    """Create labels for a given cohort. The resulting table will be under the modeling schema,
    and the name of the resulting table is <table_name>_labels.

    Keyword arguments:
        db_conn (object) -- database connection.
        database_config_dict (dict) -- dictionary with the configuration of the database.
        query (str) -- content of the query that outputs the content of the cohort of interest.
        role_name (str) -- role name. Defaults to NoneType.
    """

    # Set role to role_name
    set_role(db_conn=db_conn, role_name=role_name)

    table_content = query.format(
        source_data_schema_name=database_config_dict["source_data_schema_name"],
        source_data_table_name=database_config_dict["source_data_table_name"],
        modeling_schema_name=database_config_dict["modeling_schema_name"],
        cohort_table_name=database_config_dict["cohort_table_name"],
    )

    create_table_with_sql_query(
        db_conn=db_conn,
        schema_name=database_config_dict["modeling_schema_name"],
        table_name=database_config_dict["label_table_name"],
        table_content=table_content,
    )

    create_index(
        db_conn=db_conn,
        schema_name=database_config_dict["modeling_schema_name"],
        table_name=database_config_dict["label_table_name"],
        column_name=LABELS_TABLE_INDEX,
    )


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Get database connection.
    db_conn = get_db_conn()

    # Create labels for the cohort of interest.
    label_creator(
        db_conn=db_conn,
        database_config_dict=modeling_config["database_config"],
        query=modeling_config["label_config"]["query"],
        role_name=ROLE_NAME,
    )


# main()
