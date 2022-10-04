import censusdata
import yaml
import pandas as pd
import click

from config.project_constants import ROLE_NAME
from src.utils.sql_util import (
    get_db_conn,
    set_role,
    create_schema,
    create_index,
)


def create_table(db_conn, db_name, schema_name, file_path, table_name):
    """Create table from file_path under the database schema.

    Keyword arguments:
        db_conn (object) -- database connection.
        db_name (str) -- database name.
        schema_name (str) -- schema name.
        file_path (str) -- file path of data table.
        table_name (str) -- table name.
    """
    df = pd.read_csv(file_path)

    # Lowercase the column name and rename in pandas.
    df.columns = [i.lower() for i in df.columns]

    # If the table already exists, it will be replaced.
    df.pg_copy_to(
        schema=schema_name.lower(),
        name=table_name.lower(),
        con=db_conn,
        index=False,
        if_exists="replace",
    )
    print(
        f"Table succesfully created! database: {db_name} | schema: {schema_name.lower()} | table: {table_name.lower()}"
    )


def get_census_data_for_geography(geography, attributes_to_download_from_census):
    """Get census data and dump it into the database.
    This function depends on the package censusdata, which provides structured census data based on census codes.
    The data codes can be found using the method
        censusdata.search('acs5', 2020, 'concept', 'race')
    replacing <race> with the concept of interest to look up the code of the data of interest.

    Keyword arguments:
        geography (str) -- name of the geography granularity of interest, which could be either "state", "county", or "zip code tabulation area".
        attributes_to_download_from_census (dict) -- dictionary with the data to download and the attributes
                                                     needed to create the census data table.
    Returns:
        geography_data (pd.DataFrame) --  DataFrame with the census data of interest given a geography.

    Raises:
        ValueError -- if geography is unknown.
    """
    # Download state data for ACS 2016-2020 5-year estimates for the columns of interest.
    geography_data = censusdata.download(
        "acs5",
        2020,
        censusdata.censusgeo([(geography, "*")]),
        list(attributes_to_download_from_census.keys()),
    )
    geography_data = pd.DataFrame(geography_data)
    geography_data = geography_data.rename(columns=attributes_to_download_from_census)

    # Create column with the name of the geography of interest.
    if geography == "state":
        # The index of this geography looks like <New Mexico: Summary level: 040, state:35>.
        geography_data["state_id"] = [
            item.params()[0][1] for item in geography_data.index.to_list()
        ]
        geography_data["state"] = [
            str(item).split(":")[0] for item in geography_data.index.to_list()
        ]
        geography_data = geography_data.reset_index()
        geography_data = pd.DataFrame.reindex(
            geography_data,
            columns=["state", "state_id"]
            + list(attributes_to_download_from_census.values()),
        )
    elif geography == "zip code tabulation area":
        # The index of this geography looks like <ZCTA5 99929: Summary level: 860, zip code tabulation area:99929>.
        geography_data["zcta"] = [
            item.params()[0][1] for item in geography_data.index.to_list()
        ]
        geography_data = geography_data.reset_index()
        geography_data = pd.DataFrame.reindex(
            geography_data,
            columns=["zcta"] + list(attributes_to_download_from_census.values()),
        )
    elif geography == "county":
        # The index of this geography looks like <Todd County, Minnesota: Summary level: 050, state:27> county:153>.
        geography_data[["state_id", "state", "county_id", "county"]] = [
            (
                item.params()[0][1],
                item.name.split(",")[1].strip(),
                item.params()[1][1],
                item.name.split(",")[0].replace("County", "").strip(),
            )
            for item in geography_data.index.to_list()
        ]
        geography_data = geography_data.reset_index()
        geography_data = pd.DataFrame.reindex(
            geography_data,
            columns=["state_id", "state", "county_id", "county"]
            + list(attributes_to_download_from_census.values()),
        )
    else:
        raise TypeError(f"Unknown geography {geography}.")

    return geography_data


def download_and_dump_census_data(
    db_conn, db_name, schema_name, data_to_download_from_census
):
    """Download census data and dump it into the database. This function depends on the package censusdata,
    which provides structured census data based on census codes. The data codes can be found using the method:
        censusdata.search('acs5', 2015, 'concept', 'race')
    replacing <race> with the concept of interest to look up the code of the data of interest.

    Keyword arguments:
        db_conn (object) -- database connection.
        db_name (str) -- name of the database.
        schema_name (str) -- name of the schema of interest.
        data_to_download_from_census (dict) -- dictionary with the data to download and the attributes
                                               needed to create the census data table.
    """
    # Download data for ACS 2011-2015 5-year estimates for the columns of interest given a geography.
    for table_to_create in data_to_download_from_census["tables_to_create"]:
        df = get_census_data_for_geography(
            geography=table_to_create["geography"],
            attributes_to_download_from_census=data_to_download_from_census["content"],
        )

        # If the table already exists, it will be replaced.
        df.pg_copy_to(
            schema=schema_name.lower(),
            name=table_to_create["table_name"].lower(),
            con=db_conn,
            index=False,
            if_exists="replace",
        )
        print(
            f"Table succesfully created! database: {db_name} | schema: {schema_name.lower()} | table: {table_to_create['table_name'].lower()}."
        )

        # Set index for table.
        if (
            len(table_to_create["indexes"]) != 0
            and table_to_create["indexes"][0] is not None
        ):
            [
                create_index(
                    db_conn=db_conn,
                    schema_name=schema_name,
                    table_name=table_to_create["table_name"].lower(),
                    column_name=column_name,
                )
                for column_name in table_to_create["indexes"]
            ]


@click.command()
@click.option(
    "--config",
    prompt="Path to the .yaml file containing config info regarding data loading",
    default="config/data_to_database_config.yaml",
)
def main(config):
    """Loads data into the database according to the settings set in CONFIG.

    Keyword arguments:
        config (str) -- Path to the .yaml config file contaning info regarding data loading. Obtained via command-line argument.
    """

    with open(config) as f:
        database_config = yaml.load(f, Loader=yaml.FullLoader)
        db_name = database_config["db_name"]
        schema_name = database_config["schema_name"]
        role_name = ROLE_NAME
        tables_to_load = database_config["tables_to_load"]
        file_paths = [d["file_path"] for d in tables_to_load]

        table_names = [d["table_name"] for d in tables_to_load]
        indexes = [d["indexes"] for d in tables_to_load]
        data_to_download_from_census = database_config["data_to_download_from_census"]

        # Get database connection.
        db_conn = get_db_conn()

        # Set database role.
        if role_name is not None:
            set_role(db_conn=db_conn, role_name=role_name)

        # Create schema if it doesn't exist.
        create_schema(db_conn=db_conn, schema_name=schema_name)

        # for file_path, table_name, column_names in zip(
        #     file_paths, table_names, indexes
        # ):
        #     # Create table in schema.
        #     create_table(
        #         db_conn=db_conn,
        #         db_name=db_name,
        #         schema_name=schema_name,
        #         file_path=file_path,
        #         table_name=table_name,
        #     )

        #     # Set index for table.
        #     if len(column_names) != 0 and column_names[0] is not None:
        #         [
        #             create_index(
        #                 db_conn=db_conn,
        #                 schema_name=schema_name,
        #                 table_name=table_name,
        #                 column_name=column_name,
        #             )
        #             for column_name in column_names
        #         ]

        download_and_dump_census_data(
            db_conn=db_conn,
            db_name=db_name,
            schema_name=schema_name,
            data_to_download_from_census=data_to_download_from_census,
        )


main()
