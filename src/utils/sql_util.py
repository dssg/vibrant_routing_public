from datetime import datetime
import logging
import pandas as pd
import ohio.ext.pandas
import os
from sqlalchemy import create_engine
from config.project_constants import (
    EXPERIMENT_SCHEMA_NAME,
    EXPERIMENT_SCHEMA_NAME_ROUTING,
    ROLE_NAME,
    ROUTING_LEVEL_SCHEMA_NAME,
    SOURCE_DATA_SCHEMA_NAME,
    SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
    MODELING_CONFIG_FILE,
)
from src.utils.util import create_hash


def get_db_conn(return_engine=False):
    """Get credentials from environment variables.

    Returns:
        db_conn (object) -- database connection.
    """

    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")

    # Configure connection to postgres
    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database)
    )

    # Open a connection
    db_conn = engine.connect()

    if return_engine:
        return engine

    return db_conn


def set_role(db_conn, role_name="vibrant-routing-role"):
    """Set the current user identifier of the current session.

    Keyword arguments:
        db_conn (object) -- datebase connection.
        role_name (str) -- role name. It defaults to "vibrant-routing-role".
    """

    # Query to run.
    query = f"""
        set role "{role_name}";
    """

    try:
        db_conn.execute(query)
        logging.debug(f"Role set to {role_name}.")
    except:
        logging.error(f"Failed to set role to {role_name}.")


def create_schema(db_conn, schema_name):
    """Create schema if it doesn't already exist.

    Keyword arguments:
        db_conn (object) -- datebase connection.
        schema_name (str) -- schema name.
    """

    # Query to run.
    query = f"""
        create schema if not exists {schema_name};
    """

    try:
        db_conn.execute(query)
        logging.debug(f"{schema_name} schema created (if it doesn't already exist).")
    except:
        logging.error(f"Failed to create {schema_name} schema.")


def drop_table(db_conn, schema_name, table_name):
    """Drop table if it already exists.

    Keyword arguments:
        db_conn (object) -- datebase connection.
        schema_name (str) -- schema name.
        table_name (str) -- table name.
    """

    # Query to run.
    query = f"""
        drop table if exists {schema_name}.{table_name};
    """
    try:
        db_conn.execute(query)
        logging.debug(
            f"{schema_name}.{table_name} table successfully dropped (if it already existed)."
        )
    except:
        logging.error(f"Failed to drop {schema_name}.{table_name} table.")


def grant_permission(db_conn, schema_name, table_name, role_name):
    """Grant permission to role.

    Keyword arguments:
        db_conn (object) -- datebase connection.
        schema_name (str) -- schema name.
        table_name (str) -- table name.
        role_name (str) -- role name.
    """

    # Query to run.
    query = f"""
        GRANT ALL ON {schema_name}.{table_name} TO "{role_name}";
    """

    try:
        db_conn.execute(query)
        logging.debug(
            f"Permission of {table_name} table from {schema_name} schema has been successfully granted to {role_name}."
        )
    except:
        logging.error(
            f"Failed to grant permission of {table_name} table from {schema_name} schema to {role_name}."
        )


def create_index(db_conn, schema_name, table_name, column_name):
    """Create an index on a column of a table under a schema.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
        table_name (str) -- table name.
        column_name (str) -- column name to create the index on.
    """

    # Query to run.
    query = f"""
        CREATE INDEX ON {schema_name}.{table_name}({column_name.lower()})
    """

    try:
        db_conn.execute(query)
        logging.debug(
            f"Successfully created index on {schema_name}.{table_name}({column_name})."
        )
    except:
        logging.error(
            f"Failed to create index on {schema_name}.{table_name}({column_name})."
        )


def get_statedata(db_conn, area_code):
    """Get information about the state given an area code.

    Keyword arguments:
        db_conn (object) -- database connection.
        area_code (str) -- area code.

    Returns:
        state_data(pd.DataFrame) -- state geographical information
    """

    # Query to run.
    query = f"""
        select 
            city,
            state,
            country,
            latitude,
            longitude 
        from raw.area_codes_by_citystate acbc 
        where area_code={area_code}
        limit 1;
    """
    state_data = pd.read_sql_query(query, db_conn)
    return state_data


def get_call_center_network(db_conn, center_key):
    """Get the network distribution of call center given a call center key.

    Keyword arguments:
        db_conn (object) -- database connection.
        center_key (str) -- call center unique identifier.

    Returns:
        network (str) -- network that the center_key belongs to
    """

    # Query to run.
    query = f"""
        select 
            network
        from raw.vibrant_centers_calls_202206031630 vcc 
        where center_key = '{center_key}'
        group by 1;
    """

    network = pd.read_sql_query(query, db_conn)
    return network


def is_center_backup(db_conn, center_key):
    """Check if a call center is also a National Backup center.

    Keyword arguments:
        db_conn (object) -- database connection.
        center_key (str) -- call center unique identifier.

    Returns:
        is_backup (bool) -- whether the center at center_key is a National Backup center
    """

    result_df = get_call_center_network(db_conn, center_key)

    if "National-Backup" in result_df.values:
        is_backup = True
    else:
        is_backup = False

    return is_backup


def get_center_info(db_conn, center_key):
    """Get information about a center given an center key.

    Keyword arguments:
        db_conn (object) -- database connection.
        center_key (str) -- call center unique identifier.

    Returns:
        center_data (pd.DataFrame) -- geographical information about the center
    """

    # Query to run.
    query = f"""
        select 
            center_key,
            center_name,
            center_state,
            center_city,
            center_county,
            center_zip,
            center_fips,
            center_lat,
            center_lng,
            center_time_zone,
            center_uses_dst,
            center_visn
        from raw.vibrant_centers_calls_202206031630 vcc 
        where center_key = '{center_key}'
        limit 1;
    """
    center_data = pd.read_sql_query(query, db_conn)
    return center_data


def create_table_with_sql_query(db_conn, schema_name, table_name, table_content):
    """Create table based on a sql query.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
        table_name (str) -- table name.
        table_content (str) -- string of sql query that outputs the content of the table.
    """

    # Set role
    set_role(db_conn=db_conn, role_name=ROLE_NAME)

    # Drop table if it already exists
    drop_table(
        db_conn=db_conn,
        schema_name=schema_name,
        table_name=table_name,
    )

    # Query to run.
    query = f"""
        create table {schema_name}.{table_name} as (
            {table_content}
        )
    """
    logging.debug(f"This is the query:\n{query}")
    try:
        db_conn.execute(query)
        n_rows = get_number_of_rows_table(
            db_conn=db_conn,
            schema_name=schema_name,
            table_name=table_name,
        )
        logging.debug(
            f"{schema_name}.{table_name} table successfully created with {n_rows} rows!"
        )
    except:
        logging.error(f"{schema_name}.{table_name} table creation failed!")


def get_number_of_rows_table(db_conn, schema_name, table_name):
    """Get the number of rows of <schema_name>.<table_name>.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
        table_name (str) -- table name.

    Returns:
        number of rows (int) -- number of rows of <schema_name>.<table_name>.
    """
    # Query to run.
    query = f"select count(*) from {schema_name}.{table_name}"

    # Extract result.
    result = db_conn.execute(query)
    return result.fetchone()[0]


def add_model_entry_to_db(
    db_conn,
    model_class,
    pickle_path,
    parameters,
    label,
    features,
    split,
    train_matrix_path,
    log_path,
):
    """Add an entry to the {EXPERIMENT_SCHEMA_NAME}.models table with the specifications of the model of interest.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_class (str) -- class of the model of interest.
        pickle_path (str) -- path where the pickle file of the model is stored.
        parameters (dict) -- dict with the configuration parameters of the model.
        label (str) -- label that the model was trained to optimize.
        features (str) -- string with the set of the features that were used to train the model.
        split (dict) -- dictionary with the ends of the split.
        train_matrix_path (str) -- path where the train matrix is stored.
        log_path (str) -- complete path where the logs are saved.

    Returns:
        model_id (int) -- identifier of the model. This value is automatically created by psql.
    """
    # fetch id of row to later output model id
    query = f"""
        insert into {EXPERIMENT_SCHEMA_NAME}.models (
            model_class, 
            creation_datetime_est, 
            pickle_path, parameters, 
            "label", 
            features, 
            train_start_datetime_est, 
            train_end_datetime_est,
            train_matrix_path,
            log_path
        )    
        values (
            '{model_class}',
            '{str(datetime.now())}'::timestamp,
            '{pickle_path}',
            '{parameters}',
            '{label}',
            array{features},
            '{split["start_datetime_est"]}'::timestamp,
            '{split["end_datetime_est"]}'::timestamp,
            '{train_matrix_path}',
            '{log_path}'
        )
        returning model_id;
        """
    try:
        query_return = db_conn.execute(query)
        logging.debug(
            f"New model {model_class} entry successfully added to {EXPERIMENT_SCHEMA_NAME}.models database!"
        )
        return query_return.fetchone()[0]

    except:
        logging.error(
            f"Failed to add new {model_class} model entry to {EXPERIMENT_SCHEMA_NAME}.models database!"
        )


def get_model_id_from_path(
    db_conn,
    pickle_path,
):
    """Get the model_id from the model path.

    Keyword arguments:
        db_conn (object) -- database connection.
        pickle_path (str) -- path where the model is stored.

    Returns:
        model_id (int) -- model identifier.
    """
    # Check if experiment already exists
    model_id_query = f"""
        select model_id 
        from {EXPERIMENT_SCHEMA_NAME}.models
        where pickle_path='{pickle_path}';
        """
    model_id = db_conn.execute(model_id_query).fetchone()[0]
    return model_id


def get_experiment_id(
    db_conn,
    model_id,
    split,
    evaluation_matrix_path,
):
    """Get the experiment_id of a configuration. If it doesn't exist, it creates it in the database.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- model identifier.
        split (dict) -- dictionary with the ends of the split.
        evaluation_matrix_path (str) -- path where the evaluation matrix is stored.

    Returns:
        experiment_id (int) -- experiment identifier.
    """
    # Check if experiment already exists
    experiment_id_query = f"""
        select experiment_id 
        from {EXPERIMENT_SCHEMA_NAME}.evaluations
        where 
            model_id={model_id} and
            evaluation_start_datetime_est='{split["start_datetime_est"]}' and
            evaluation_end_datetime_est='{split["end_datetime_est"]}';
        """
    experiment_id = db_conn.execute(experiment_id_query).fetchone()

    if experiment_id is None:
        query = f"""
            insert into {EXPERIMENT_SCHEMA_NAME}.evaluations (
                model_id, 
                creation_datetime_est,
                evaluation_start_datetime_est,
                evaluation_end_datetime_est,
                evaluation_matrix_path
            )    
            values (
                {model_id},
                '{str(datetime.now())}'::timestamp,
                '{split["start_datetime_est"]}'::timestamp,
                '{split["end_datetime_est"]}'::timestamp,
                '{evaluation_matrix_path}'
            )
            returning experiment_id;
            """
        try:
            query_return = db_conn.execute(query)
            logging.debug(
                f"Experiment successfully added to {EXPERIMENT_SCHEMA_NAME}.evaluations!"
            )
            return query_return.fetchone()[0]

        except:
            logging.error(
                f"Failed to add experiment to {EXPERIMENT_SCHEMA_NAME}.evaluations!"
            )
    return experiment_id[0]


def add_metric_entry_to_db(
    db_conn,
    experiment_id,
    model_id,
    evaluation_metrics,
):
    """Add an entry to the {EXPERIMENT_SCHEMA_NAME}.metrics table with the metrics of the experiment of interest.

    Keyword arguments:
        db_conn (object) -- database connection.
        experiment_id (int) -- experiment identifier.
        model_id (int) -- model identifier.
        evaluation_metrics (dict) -- dictionary with the evaluated metrics and their values.
    """
    # Loop over all the evaluation metrics.
    for metric, values in evaluation_metrics.items():
        if len(metric.split("=")) == 2:
            metric_name, k = metric.split("=")
        else:
            metric_name = metric
            k = "NULL"

        query = f"""
            insert into {EXPERIMENT_SCHEMA_NAME}.metrics (
                experiment_id,
                model_id, 
                creation_datetime_est,
                metric,
                k,
                best_value,
                worst_value,
                stochastic_value,
                stochastic_std
            )    
            values (
                {experiment_id},
                {model_id},
                '{str(datetime.now())}'::timestamp,
                '{metric_name}',
                {k},
                NULL,
                {values["worst_value"]},
                NULL,
                NULL
            );
            """
        try:
            db_conn.execute(query)
            logging.debug(
                f"{metric}={values} successfully added to {EXPERIMENT_SCHEMA_NAME}.metrics!"
            )

        except:
            logging.error(
                f"Failed to add {metric}={values} to {EXPERIMENT_SCHEMA_NAME}.metrics!"
            )


def add_feature_importance_to_db(feature_rankings):
    """Add an entry to the {EXPERIMENT_SCHEMA_NAME}.feature_importance table with
    the values that characterize the importance of the model features.

    Keyword arguments:
        feature_rankings (pd.DataFrame) -- DataFrame with the feature importance rankings.
    """
    # Loop over all the evaluation metrics.
    try:
        # Use the method pg_copy_to from ohio to copy data in bulk.
        engine = get_db_conn(return_engine=True)
        with engine.connect() as conn:
            with conn.begin():
                feature_rankings.pg_copy_to(
                    name="feature_importance",
                    schema=EXPERIMENT_SCHEMA_NAME,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
        logging.debug(
            f"Entry successfully added to {EXPERIMENT_SCHEMA_NAME}.feature_importance!"
        )
    except:
        logging.error(
            f"Failed to add entry to {EXPERIMENT_SCHEMA_NAME}.feature_importance!"
        )


def add_predictions_to_db(
    experiment_id,
    model_id,
    y_index,
    y_true,
    y_predicted,
):
    """Add an entry to the {EXPERIMENT_SCHEMA_NAME}.predictions table with the predictions of the experiment of interest.

    Keyword arguments:
        experiment_id (int) -- experiment identifier.
        model_id (int) -- model identifier.
        y_index (np.ndarray) -- indexes.
        y_true (np.ndarray) -- true labels.
        y_predicted (np.ndarray) -- predicted labels.
    """
    results = pd.DataFrame(
        list(
            zip(
                [experiment_id] * len(y_true),
                [model_id] * len(y_true),
                y_index,
                y_true,
                y_predicted,
            )
        ),
        columns=[
            "experiment_id",
            "model_id",
            "routing_attempts_id",
            "y_true",
            "y_predicted",
        ],
    )

    try:
        # Use the method pg_copy_to from ohio to copy data in bulk.
        engine = get_db_conn(return_engine=True)
        with engine.connect() as conn:
            with conn.begin():
                results.pg_copy_to(
                    name="predictions",
                    schema=EXPERIMENT_SCHEMA_NAME,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
        logging.debug(
            f"Entry successfully added to {EXPERIMENT_SCHEMA_NAME}.predictions!"
        )
    except:
        logging.error(f"Failed to add entry to {EXPERIMENT_SCHEMA_NAME}.predictions!")


def get_metrics_from_db(
    db_conn,
    experiment_id,
    model_id,
):
    """Get the calculated metrics for a given experiment_id and model_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        experiment_id (int) -- identifier of the experiment of interest.
        model_id (int) -- identifier of the model of interest.

    Returns:
        metrics (pd.DataFrame) -- pd.DataFrame with the stored metrics.
    """
    # Check if experiment already exists
    metrics_query = f"""
        select metric, k, best_value, worst_value, stochastic_value, stochastic_std 
        from {EXPERIMENT_SCHEMA_NAME}.metrics
        where experiment_id={experiment_id} and model_id={model_id};
        """
    metrics_data = pd.DataFrame(db_conn.execute(metrics_query).fetchall())
    metrics_data[["metric", "type"]] = metrics_data.metric.str.split("@", expand=True)
    metrics_data = metrics_data.set_index(["metric", "type"])
    return metrics_data


def get_predictions_from_db(db_conn, experiment_id, model_id, return_id=False):
    """Get the predictions for a given experiment_id and model_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        experiment_id (int) -- identifier of the experiment of interest.
        model_id (int) -- identifier of the model of interest.
        return_id (bool) -- boolean value to indicate whether the id (routing_attempts_id) should be returned or not.
                            It defaults to False.

    Returns:
        predictions (pd.DataFrame) -- pd.DataFrame with the stored predictions.
    """
    # Check if experiment already exists
    predictions_query = f"""
        select routing_attempts_id, y_true, y_predicted 
        from {EXPERIMENT_SCHEMA_NAME}.predictions
        where experiment_id={experiment_id} and model_id={model_id};
        """
    predictions_data = db_conn.execute(predictions_query).fetchall()

    if return_id:
        return pd.DataFrame(predictions_data)
    return pd.DataFrame(predictions_data)[["y_true", "y_predicted"]]


def get_topk_models(
    db_conn,
    k=1,
    metric_of_interest="auc-roc",
    value_of_interest="worst_value",
    creation_start_datetime=None,
    creation_end_datetime=None,
):
    """Get the top k best-performing models.

    Keyword arguments:
        db_conn (object) -- database connection.
        k (int) -- number of models to return.
        metric_of_interest (str) -- metric that the prediction was evaluated on.
                                    Must be one of (
                                                        "auc-roc",
                                                        "precision@pct", "precision@abs",
                                                        "recall@pct", "recall@abs",
                                                        "fpr@pct", "fpr@abs"
                                                    )
                                    Defaults to "auc-roc".
        value_of_interest (str) -- value to use to pick the best model.
        creation_start_datetime (datetime) --  timestamp of the creation start date. Defaults to NoneType.
        creation_end_datetime (datetime) -- timestamp of the creation end date. Defaults to NoneType.

    Returns:
        ranked_models (list) -- list of length k, containing the best-performing models in order of descent.
    """
    # Validate that the given metric is a valid metric.
    valid_metrics = (
        "auc-roc",
        "precision@pct",
        "precision@abs",
        "recall@pct",
        "recall@abs",
        "fpr@pct",
        "fpr@abs",
    )
    assert (
        metric_of_interest in valid_metrics
    ), f"{metric_of_interest} must be one of {valid_metrics}"

    # If datetime = None, assign value to cover whole interval.
    if creation_start_datetime is None:
        creation_start_datetime = "1900-01-01"
    if creation_end_datetime is None:
        creation_end_datetime = datetime.now()

    # Set query.
    query = f"""
            select
                e.experiment_id,
                model_id,
                model_class,
                best_value,
                worst_value,
                stochastic_value,
                k
            from {EXPERIMENT_SCHEMA_NAME}.metrics m 
            left join {EXPERIMENT_SCHEMA_NAME}.models m2 
                using (model_id)
            left join {EXPERIMENT_SCHEMA_NAME}.evaluations e 
                using (model_id)
            where 
                m.metric = '{metric_of_interest}'
                and m2.creation_datetime_est 
                between '{creation_start_datetime}' and 
                '{creation_end_datetime}'
            group by e.experiment_id, model_id, model_class, best_value, worst_value, stochastic_value, k
            order by {value_of_interest} desc
            """
    ranked_models = pd.read_sql_query(query, db_conn)
    return ranked_models.head(k)


def get_best_model_from_model_class(
    db_conn,
    model_class=["AnswerRateAtCenter"],
    metric_of_interest="auc-roc",
    value_of_interest="worst_value",
    creation_start_datetime=None,
    creation_end_datetime=None,
):
    """Get best model from list of model class.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_class (list) -- list of the classes of interest to pick the best model from.
                              Must be one of `<modeling_config["model_grid"]>.keys().split(".")[-1]`
                              Defaults to ["AnswerRateAtCenter"].
        metric_of_interest (str) -- metric that the prediction was evaluated on.
                                    Must be one of (
                                                        "auc-roc",
                                                        "precision@pct", "precision@abs",
                                                        "recall@pct", "recall@abs",
                                                        "fpr@pct", "fpr@abs"
                                                    )
                                    Defaults to "auc-roc".
        value_of_interest (str) -- value to use to pick the best model.
        creation_start_datetime (datetime) --  timestamp of the creation start date. Defaults to NoneType.
        creation_end_datetime (datetime) -- timestamp of the creation end date. Defaults to NoneType.
    Returns:
        best_model_from_model_class (pd.DataFrame) -- pandas dataframe containing the result of best-performing model in model class.
    """
    # Validate that the given metric is a valid metric.
    valid_metrics = (
        "auc-roc",
        "precision@pct",
        "precision@abs",
        "recall@pct",
        "recall@abs",
        "fpr@pct",
        "fpr@abs",
    )
    assert (
        metric_of_interest in valid_metrics
    ), f"{metric_of_interest} must be one of {valid_metrics}"

    # If datetime = None, assign value to cover whole interval.
    if creation_start_datetime is None:
        creation_start_datetime = "1900-01-01"
    if creation_end_datetime is None:
        creation_end_datetime = datetime.now()

    # Set query.
    query = f"""
            select 
                e.experiment_id,
                model_id,
                model_class,
                best_value,
                worst_value,
                stochastic_value,
                k
            from {EXPERIMENT_SCHEMA_NAME}.metrics m 
            left join {EXPERIMENT_SCHEMA_NAME}.models m2 
                using (model_id)
            left join {EXPERIMENT_SCHEMA_NAME}.evaluations e 
                using (model_id)
            where 
            m.metric = '{metric_of_interest}'
            and m2.model_class in {tuple(model_class)}
            and m2.creation_datetime_est 
                between '{creation_start_datetime}' and 
                '{creation_end_datetime}'
            group by e.experiment_id, model_id, model_class, best_value, worst_value, stochastic_value, k
            order by {value_of_interest} desc
            """

    best_model_from_model_class = pd.read_sql_query(query.format(model_class), db_conn)
    return best_model_from_model_class.head(1)


def get_best_model_per_model_class(
    db_conn,
    model_class=["AnswerRateAtCenter"],
    metric_of_interest="auc-roc",
    value_of_interest="worst_value",
    creation_start_datetime=None,
    creation_end_datetime=None,
):
    """Get best model for each given model class.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_class (list) -- list of the classes of interest to pick the best model from.
                              Must be one of `<modeling_config["model_grid"]>.keys().split(".")[-1]`
                              Defaults to ["AnswerRateAtCenter"].
        metric_of_interest (str) -- metric that the prediction was evaluated on.
                                    Must be one of (
                                                        "auc-roc",
                                                        "precision@pct", "precision@abs",
                                                        "recall@pct", "recall@abs",
                                                        "fpr@pct", "fpr@abs"
                                                    )
                                    Defaults to "auc-roc".
        value_of_interest (str) -- value to use to pick the best model.
        creation_start_datetime (datetime) --  timestamp of the creation start date. Defaults to NoneType.
        creation_end_datetime (datetime) -- timestamp of the creation end date. Defaults to NoneType.
    Returns:
        best_model_from_model_class (pd.DataFrame) -- pandas dataframe containing the result of
                                                      best-performing model in each model class.
    """
    # Validate that the given metric is a valid metric.
    valid_metrics = (
        "auc-roc",
        "precision@pct",
        "precision@abs",
        "recall@pct",
        "recall@abs",
        "fpr@pct",
        "fpr@abs",
    )
    assert (
        metric_of_interest in valid_metrics
    ), f"{metric_of_interest} must be one of {valid_metrics}"

    # If datetime = None, assign value to cover whole interval.
    if creation_start_datetime is None:
        creation_start_datetime = "1900-01-01"
    if creation_end_datetime is None:
        creation_end_datetime = datetime.now()

    # Set query.
    query = f"""
            select 
                model_class,
                max({value_of_interest}) as max_{value_of_interest}
            from {EXPERIMENT_SCHEMA_NAME}.metrics m 
            left join {EXPERIMENT_SCHEMA_NAME}.models m2 
                using (model_id)
            left join {EXPERIMENT_SCHEMA_NAME}.evaluations e 
                using (model_id)
            where 
            m.metric = '{metric_of_interest}'
            and m2.model_class in {tuple(model_class)}
            and m2.creation_datetime_est 
                between '{creation_start_datetime}' and 
                '{creation_end_datetime}'
            group by model_class
            order by max({value_of_interest}) desc
            """

    best_model_per_model_class = pd.read_sql_query(query.format(model_class), db_conn)
    return best_model_per_model_class


def get_saved_model_info_from_db(db_conn, model_id, info_to_get=None):
    """Handy function to get information about the saved models.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- identifier of the model of interest.
        info_to_get (str) -- information of interest. If None, it returns all the columns.
                             Must be one of ("model_class", "creation_datetime_est",
                                             "pickle_path", "parameters",
                                             "label", "features", "train_matrix_path",
                                             "train_start_datetime_est", "train_end_datetime_est").
                            Defaults to NoneType.

    Returns:
        info_about_saved_model (pd.DataFrame or np.ndarray) -- pd.DataFrame or np.ndarray with information about the saved model.
    """
    query = f"""
                select *
                from {EXPERIMENT_SCHEMA_NAME}.models
                where model_id = {model_id};
            """

    info_about_saved_model = pd.read_sql_query(query.format(model_id), db_conn)
    if info_to_get is not None:
        info_about_saved_model = info_about_saved_model[info_to_get].values
        return info_about_saved_model
    else:
        return info_about_saved_model


def get_wait_time_from_center(
    db_conn,
    center_key,
    termination_number,
):
    """Get the waiting time for a given combination of center_key and termination_number.
    Disclaimer: in case the combination of center_key and termination_number doesn't exist,
    the function will return '3 minutes' as it is the maximum waiting time.

    Keyword arguments:
        db_conn (object) -- database connection.
        center_key (str) -- identifier of the call center.
        termination_number (int) -- termination number.

    Returns:
        waiting time (int) -- anticipated waiting time in minutes.
    """
    wait_time_query = f"""
        select wait_time 
        from {ROUTING_LEVEL_SCHEMA_NAME}.center_waiting_times
        where center_key='{center_key}' and termination_number={termination_number};
        """
    wait_time = db_conn.execute(wait_time_query).fetchone()

    if wait_time:
        return wait_time[0]
    else:
        return 3


def get_abandonment_probability_by_minutes(db_conn):
    """Get the estimated abandonment probability for a call within 1 minute window.

    Keyword arguments:
        db_conn (object) -- database connection.

    Returns:
        abandonment probability by minutes (dict) -- estimated abandonment probability for waiting 19+ minutes.
    """
    query = f"""
        select prob_abandon
        from {ROUTING_LEVEL_SCHEMA_NAME}.abandon_prob_by_bucket 
        order by bucket_start_sec asc
    """

    logging.debug(f"This is the query:\n{query}")
    try:
        abandon_proba_by_minute = pd.read_sql_query(query, db_conn)["prob_abandon"]
        abandon_proba_by_minute = {k + 1: v for k, v in abandon_proba_by_minute.items()}
        return abandon_proba_by_minute
    except:
        logging.error(f"Failed to get probability of abandonment by minutes!")
        raise ValueError(f"Failed to get probability of abandonment by minutes!")


def center_historical_disposition_estimate(db_conn):
    """Compute an estimated average for call center historical disposition for all the calls routed to them.
    The query that created the <center_historical_disposition_stat> table can be found in the config file under
    <routing_level_config["tables_to_create"]>.
    It calculates the average ring time, time to answer, time to leave and talk time for any center and termination pair.
    It filters the query based on whether or not the call was answered, abandoned, or flowed out.

    Keyword arguments:
        db_conn (object) -- database connection.

    Returns:
        center historical disposition estimate (pd.DataFrame) -- average estimate of what happened to past calls in a particular center.
    """
    query = f"""
        select *
        from {ROUTING_LEVEL_SCHEMA_NAME}.center_historical_disposition_stat 
    """

    logging.debug(f"This is the query:\n{query}")
    try:
        center_hist_disp_estimate = pd.read_sql_query(query, db_conn)
        center_hist_disp_estimate = center_hist_disp_estimate.set_index(
            keys=["center_key", "termination_number"]
        )
        return center_hist_disp_estimate
    except:
        logging.error(f"Failed to get center historical disposition estimate!")
        raise ValueError(f"Failed to get center historical disposition estimate!")


def get_center_info(db_conn, center_key, termination_number):
    """Get information about a center.
    The function assumes that we have created the <center_lookup> table.
        Here's an example query that was run on dbeaver.
            create table processed.center_lookup as (
                select
                    distinct termination_number,
                    center_key, center_name,
                    center_state_abbrev, center_state_full_name, center_county,
                    center_state_population_size, center_state_suicides_2020,
                    center_time_zone, center_uses_dst, center_visn
                from processed.routing_attempts ra
                order by center_key asc
            )

    Keyword arguments:
        db_conn (object) -- database connection.
        center_key (str) -- alphanumeric value that identifies a center.
        termination_number (int) -- value that uniquely identifies a center.

    Returns:
        center infomation (dict) -- a dictionary with details about the center like timezone, state, etc.
    """
    query = f"""
        select 
            center_state_abbrev,
            center_uses_dst,
            center_time_zone,
            center_uses_dst 
        from {SOURCE_DATA_SCHEMA_NAME}.center_lookup
        where center_key = '{center_key}' and termination_number = {termination_number}
    """

    logging.debug(f"This is the query:\n{query}")
    try:
        center_info = pd.read_sql_query(query, db_conn).to_dict(orient="index")[0]
        return center_info
    except:
        logging.error(f"Failed to get center info!")
        raise ValueError(f"Failed to get center info!")


def get_number_nspl_in_state(db_conn, state_abbrev):
    """Get the computed number of local (nspl) call centers in a state.
    The function assumes that we have created the <state_center_data> table and that
    there is a column called <num_nspl_centers_in_center_state>.

    Keyword arguments:
        db_conn (object) -- database connection.
        state_abbrev (str) -- abbreviation of each state.

    Returns:
        num_nspl_in_state (int) -- number of nspl call centers in <state_abbrev> state.
    """
    query = f"""
                select num_nspl_centers_in_center_state 
            from {SOURCE_DATA_SCHEMA_NAME}.state_center_data scd 
            where state_abbrev  = '{state_abbrev}'
    """

    logging.debug(f"This is the query:\n{query}")
    try:
        num_nspl_in_state = pd.read_sql_query(query, db_conn)[
            "num_nspl_centers_in_center_state"
        ][0]
        return num_nspl_in_state
    except:
        logging.error(f"Failed to get the number of NSPL call centers in state!")
        raise ValueError(f"Failed to get the number of NSPL call centers in state!")


def get_caller_info(db_conn, call_key):
    """Get information about a caller given the <call_key>.
    The function assumes that we have created the <active_calls_in_queue> table and that
    there are columns in this table that give details about the incoming call, like their
    state, timezone and whether or not they're calling from a cell phone.

    Keyword arguments:
        db_conn (object) -- database connection.
        call_key (str) -- unique identifier of a caller.

    Returns:
        caller_info (int) -- infomation about the caller with this <call_key>.
    """
    query = f"""
        select 
            caller_is_cell_phone,
            caller_state_abbrev,
            caller_time_zone 
        from {ROUTING_LEVEL_SCHEMA_NAME}.active_calls_in_queue aciq 
        where call_key = '{call_key}'
    """

    logging.debug(f"This is the query:\n{query}")
    try:
        caller_info = pd.read_sql_query(query, db_conn).to_dict(orient="index")[0]
        return caller_info
    except:
        logging.error(f"Failed to get caller info!")
        raise ValueError(f"Failed to get caller info!")


class ModifyDBTable(object):
    def __init__(
        self,
        db_conn,
        schema_name,
        table_name,
        role_name=None,
    ):
        """Database utility class with methods that
            * insert data into table
            * update row in a table where <condition> is true
            * select columns from table where <condition> is true
            * delete row from table where <condition> is true

        Keyword arguments:
            db_conn (object) -- database connection.
            schema_name (str) -- name of schema where table is located.
            table_name (str) -- name of table to modify.
            role_name (str, optional) -- role name. Defaults to NoneType.

        Example usage:
            modify_db_table = ModifyDBTable(
                    db_conn=db_conn,
                    schema_name="processed",
                    table_name="routing_attempts",
                    role_name="vibrant-routing-role"
                )
            result = modify_db_table.select_data_from_table(
                            row_identifier={"routing_attempts_id": 5472245},
                            columns_to_select="call_key, caller_npanxx",
                )
        """

        self.db_conn = db_conn
        self.schema_name = schema_name
        self.table_name = table_name

        # Set role to role_name.
        if role_name is not None:
            set_role(db_conn=self.db_conn, role_name=role_name)

    def insert_data_into_table(self, data):
        """Insert given data into the <schema_name>.<table_name>.
        * This does not overwrite the existing data in <schema_name>.<table_name>.
        * Every insertion appends new data to <schema_name>.<table_name>.

        Keyword arguments:
            data (dict) -- dictionary with the column name(s) as <data>.keys()
                            and value(s) to insert as <data>.values().

                            Example of data:
                                data = {
                                    "call_key": "e4011-20220526002924403-500",
                                    "arrived_datetime_est": "2022-05-26 00:29:24.000",
                                    "center_key": "IL460000",
                                    "termination_number": 6304823616,
                                }
        """
        column_names, values_to_insert = zip(*data.items())

        # Format column_name as comma-seperated string.
        column_names = ",".join(column_names)

        # Query to run.
        query = f"""
            insert into 
                {self.schema_name}.{self.table_name} ({column_names})
            values {values_to_insert};
        """
        logging.debug(f"This is the query:\n{query}")
        try:
            self.db_conn.execute(query)
            logging.debug(
                f"{values_to_insert} successfully inserted into {self.schema_name}.{self.table_name} ({column_names})!"
            )
        except:
            logging.error(
                f"Failed to insert {values_to_insert} into {self.schema_name}.{self.table_name} ({column_names})!"
            )

    def update_rows_in_table(self, data, row_identifier):
        """Update given data in table where <row_identifier> is true.

        Keyword arguments:
            data (Union(dict, str)) -- data to be updated. If type(<data>) is not str, the data
                                        is formatted to the form that psql is expecting.
                                        The dictionary expects the column names as
                                        data.keys() and values to be updated as data.values().
            row_identifier (Union(dict, str)) -- data that identifies a given row.
                                                If type(<row_identifier>) is not str,
                                                the data is formatted to the form that psql is expecting.
                                                The dictionary expects the column names as row_identifier.keys()
                                                and associated values as row_identifier.values().

        """
        # Format data to be inserted if the type of data  is a dictionary.
        if type(data) == dict:
            data = (
                str(data)
                .replace("{'", "")
                .replace("}", "")
                .replace("':", "=")
                .replace(", '", ",")
            )

        # Format row identifier if the type of data  is a dictionary.
        if type(row_identifier) == dict:
            row_identifier = (
                str(row_identifier)
                .replace("{'", "")
                .replace("}", "")
                .replace("':", "=")
                .replace(", '", " and ")
            )

        # Query to run.
        query = f"""
            update  {self.schema_name}.{self.table_name}
            set {data}
            where {row_identifier};
        """
        logging.debug(f"This is the query:\n{query}")
        try:
            self.db_conn.execute(query)
            logging.debug(
                f"{self.schema_name}.{self.table_name} where {row_identifier} successfully updated with {data}!"
            )
        except:
            logging.error(
                f"Failed to update {self.schema_name}.{self.table_name} where {row_identifier}!"
            )

    def delete_row_from_table(self, row_identifier):
        """Delete data in table where <row_identifier> is true.

        Keyword arguments:
            row_identifier (Union(dict, str)) -- data that identifies a given row.
                                                If type(<row_identifier>) is not str,
                                                the data is formatted to the form that sql is expecting.
                                                The dictionary expects the column names as row_identifier.keys()
                                                and associated values as row_identifier.values().

        """
        # Format row_identifier if the type is dictionary.
        if type(row_identifier) == dict:
            row_identifier = (
                str(row_identifier)
                .replace("{'", "")
                .replace("}", "")
                .replace("':", "=")
                .replace(", '", " and ")
            )

        # Query to run.
        query = f"""
            delete from {self.schema_name}.{self.table_name}
            where {row_identifier};
        """
        logging.debug(f"This is the query:\n{query}")
        try:
            self.db_conn.execute(query)
            logging.debug(
                f"{self.schema_name}.{self.table_name} where {row_identifier} successfully deleted!"
            )
        except:
            logging.error(
                f"Failed to delete from {self.schema_name}.{self.table_name} where {row_identifier}!"
            )

    def select_row_from_table(self, row_identifier, columns_to_select="*"):
        """Select given columns in table where <row_identifier> is true.

        Keyword arguments:
            row_identifier (Union(dict, str)) -- data that identifies a given row.
                                                If type(<row_identifier>) is not str,
                                                the data is formatted to the form that sql is expecting.
                                                The dictionary expects the column names as row_identifier.keys()
                                                and associated values as row_identifier.values().
            columns_to_select (str, optional) -- comma-seperated columns to be selected from table. Defaults to "*" i.e all columns.
                                                 Example:
                                                    columns_to_select = "call_key, caller_npanxx"
        Returns:
            result (pd.DataFrame) -- pandas dataframe of the specified columns and the associated values.
        """
        # Delete any leading or trailing commas.
        columns_to_select = columns_to_select.strip(",")

        # Format row_identifier if the type is dictionary.
        if type(row_identifier) == dict:
            row_identifier = (
                str(row_identifier)
                .replace("{'", "")
                .replace("}", "")
                .replace("':", "=")
                .replace(", '", " and ")
            )

        # Query to run.
        query = f"""
            select {columns_to_select}
            from {self.schema_name}.{self.table_name}
            where {row_identifier};
        """
        logging.debug(f"This is the query:\n{query}")
        try:
            result = pd.read_sql_query(query, self.db_conn)
            logging.debug(
                f"{columns_to_select} where {row_identifier} from {self.schema_name}.{self.table_name}"
                f"successfully selected! \n This is the result: {result}"
            )
            return result
        except:
            logging.error(
                f"Failed to select {columns_to_select} from"
                f"{self.schema_name}.{self.table_name} where {row_identifier}!"
            )

    def get_column_name(self):
        """Returns column_names (list) -- column names"""

        # Query to run.
        query = f"""
            select 
                column_name
            from information_schema.columns 
            where table_schema = '{self.schema_name}'
                and table_name = '{self.table_name}';
        """
        logging.debug(f"This is the query:\n{query}")
        column_names = pd.read_sql_query(query, self.db_conn)
        column_names = column_names["column_name"].values
        return column_names


def get_predictions_with_census_data(
    db_conn, experiment_id, model_id, geography="state"
):
    """Get the predictions for a given experiment_id and model_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        experiment_id (int) -- identifier of the experiment of interest.
        model_id (int) -- identifier of the model of interest.
        geography (str) -- geography from which to get the census data.
                           It defaults to <state>.

    Returns:
        predictions with census data (pd.DataFrame) -- pd.DataFrame with the stored predictions joined to callers' census data.
    """
    # Set the expression to use to join tables.
    if geography == "state":
        join_on = "ra.caller_state_full_name = cd.state"
    elif geography == "zcta":
        join_on = "ra.caller_zip = cd.zcta::int"
    elif geography == "county":
        join_on = "ra.caller_county = cd.county"
    else:
        logging.error(f"Unknown geography:{geography}.")

    # Set query to join predictions and census data.
    pred_and_census_query = f"""
        select p.routing_attempts_id, y_true, y_predicted, cd.*
        from {EXPERIMENT_SCHEMA_NAME}.predictions p
        left join {SOURCE_DATA_SCHEMA_NAME}.{SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME} ra using(routing_attempts_id)
        left join {SOURCE_DATA_SCHEMA_NAME}.census_data_{geography} cd on {join_on} 
        where model_id = {model_id} and experiment_id = {experiment_id};
        """
    pred_and_census_data = db_conn.execute(pred_and_census_query).fetchall()
    return pd.DataFrame(pred_and_census_data)


def get_predictions_with_feature_data(db_conn, model_id, experiment_id):
    """Get the predictions for a given experiment_id and model_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- identifier of the model of interest.
        experiment_id (int) -- identifier of the experiment of interest.

    Returns:
        predictions with feature data (pd.DataFrame) -- pd.DataFrame with the stored predictions joined to features.
    """
    # Set query to get evaluation matrix.
    evaluation_matrix_query = f"""
        select evaluation_matrix_path 
        from {EXPERIMENT_SCHEMA_NAME}.evaluations m 
        where model_id = {model_id} and experiment_id = {experiment_id};
        """
    matrix_path = db_conn.execute(evaluation_matrix_query).fetchone()[0]
    matrix = pd.read_csv(matrix_path, index_col=False)

    # Get predictions.
    predictions = get_predictions_from_db(
        db_conn=db_conn, experiment_id=experiment_id, model_id=model_id, return_id=True
    )

    # Left join predictions and evaluation data.
    df = predictions.merge(matrix, how="left", on="routing_attempts_id")

    return df


def get_predictions_with_source_data(db_conn, model_id, experiment_id):
    """Get the predictions for a given experiment_id and model_id merged with the source data.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- identifier of the model of interest.
        experiment_id (int) -- identifier of the experiment of interest.

    Returns:
        predictions with source data (pd.DataFrame) -- pd.DataFrame with the stored predictions joined to source data.
    """
    # Set query to join predictions and census data.
    query = f"""
        select *
        from {EXPERIMENT_SCHEMA_NAME}.predictions p 
        left join processed.routing_attempts ra using(routing_attempts_id)
        where model_id = {model_id} and experiment_id = {experiment_id};
        """
    predictions_with_source_data = db_conn.execute(query).fetchall()
    return pd.DataFrame(predictions_with_source_data)


def get_calibration_model(db_conn, model_id, round_value=1):
    """Get the values that characterize the calibration status of the model given a model_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_id (int) -- identifier of the model of interest.
        round (int) -- value that describes

    Returns:
        predictions with feature data (pd.DataFrame) -- pd.DataFrame with the stored predictions joined to features.
    """
    calibration_query = f"""
        select 
            round(y_predicted::numeric, {round_value}),
            avg(y_predicted) as avg_y_pred, 
            avg(y_true) as avg_y_true, 
            count(*), 
            sum(y_true) as sum_y_true
        from {EXPERIMENT_SCHEMA_NAME}.predictions p 
        where model_id = {model_id}
        group by 1
        order by 1 asc;
        """

    calibration_data = db_conn.execute(calibration_query).fetchall()
    return pd.DataFrame(calibration_data)


def add_routing_evaluation_entry_to_db(
    db_conn,
    model_path,
    trial_number,
    routing_table_path,
    config_routing,
    config_feature,
    random_seed,
    log_path,
):
    """Add an entry to the {EXPERIMENT_SCHEMA_NAME_ROUTING}.evaluations table
    with the specifications of the evaluation of interest.

    Keyword arguments:
        db_conn (object) -- database connection.
        model_path (str) -- path to trained model.
        trial_number (int) -- number of times the same experiment is being re-run.
        routing_table_path (str) -- path that points to the the routing table used.
        config_routing (dict)-- dictionary with the configuration of the routing.
        config_feature (dict)-- dictionary with the configuration of the features.
        random_seed (int) -- random seed used in the simulator.
        log_path (str) -- path to experiment logs.

    Returns:
        evaluation_id (int) -- identifier of the evaluation of interest.
    """
    # Load active calls data based on input from config routing level.
    active_calls = db_conn.execute(
        f"select * from {config_routing['database_config']['schema_name']}.active_calls_in_queue"
    )
    active_calls = pd.DataFrame(active_calls)

    # Hash characterization of evaluation.
    evaluation_hash = create_hash(
        {
            "model_path": str(model_path),
            "trial_number": str(trial_number),
            "routing_table_path": str(routing_table_path),
            "config_routing": str(config_routing),
            "config_feature": str(config_feature),
            "random_seed": str(random_seed),
            "active_calls": str(active_calls.to_dict()),
        }
    )

    query = f"""
        insert into {EXPERIMENT_SCHEMA_NAME_ROUTING}.evaluations (
            model_path, 
            trial_number,
            creation_datetime_utc,
            routing_table_path, 
            config_routing_hash, 
            config_feature_hash, 
            random_seed, 
            active_calls_start_datetime_est,
            active_calls_end_datetime_est,
            active_calls_keys,
            active_calls_count,
            evaluation_config_hash,
            log_path
        )    
        values (
            '{str(model_path)}',
            {trial_number},
            '{str(datetime.now())}'::timestamp,
            '{routing_table_path}',
            '{create_hash({"config_routing": str(config_routing)})}',
            '{create_hash({"config_feature": str(config_feature)})}',
            {random_seed},
            '{min(active_calls["initiated_datetime_est"])}'::timestamp,
            '{max(active_calls["initiated_datetime_est"])}'::timestamp,
            array{active_calls.call_key.to_list()},
            {active_calls.shape[0]},
            '{evaluation_hash}',
            '{log_path}'
        )
        returning evaluation_id;
        """
    try:
        query_return = db_conn.execute(query)
        evaluation_id = query_return.fetchone()[0]
        logging.debug(
            f"New routing evaluation {evaluation_id} entry successfully added to {EXPERIMENT_SCHEMA_NAME_ROUTING}.evaluations database!"
        )
        return evaluation_id
    except:
        logging.error(
            f"Failed to add new routing evaluation entry to {EXPERIMENT_SCHEMA_NAME_ROUTING}.evaluations database!"
        )


def main():
    """Function that exemplifies the usage of some of the functions."""
    import yaml

    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    add_routing_evaluation_entry_to_db(
        db_conn=get_db_conn(),
        model_path="/mnt/data/projects/vibrant-routing/dev_model_governance/models/1163a4f816099e875654aa0102c3670d.pickle",
        routing_table_path="",
        config_routing=modeling_config["routing_level_config"],
        config_feature=modeling_config["feature_config"],
        random_seed=2,
    )


# main()
