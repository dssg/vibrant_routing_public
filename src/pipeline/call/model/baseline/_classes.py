import json
import logging

import numpy as np
import pandas as pd
import yaml
from config.project_constants import (
    MODELING_CONFIG_FILE,
    MODELING_SCHEMA_NAME,
    ROLE_NAME,
    SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
    SOURCE_DATA_SCHEMA_NAME,
    SPLIT_FILE,
)
from src.utils.sql_util import create_table_with_sql_query, get_db_conn, set_role
from src.utils.util import fill_na

__all__ = ["AnswerRateAtCenter"]


class AnswerRateAtCenter:
    def __init__(
        self,
        db_conn,
        table_name,
        label_column_name,
        train_query,
        validation_query,
        imputation_strategy="mean",
    ):
        """Baseline that calculates the answer rate at each call center.

        Keyword arguments:
            db_conn (object) -- database connection.
            table_name (str) -- table name for the result of the `train_query`.
            label_column_name (str) -- name of the column with the result of the `train_query`.
            train_query (str) -- content of the query that computes the baseline.
            validation_query (str) -- content of the query that gets the result of the validation data.
            imputation_strategy (str, optional) -- imputation strategy to use. Defaults to "mean".

        Constants:
            SOURCE_DATA_SCHEMA_NAME (str) -- schema name of the source data.
            SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME (str) -- table name of the source data.
            MODELING_SCHEMA_NAME (str) -- schema name where the result of the `train_query` is stored.
            ROLE_NAME (str) -- name of the role that creates the table in the database.
        """

        # Keyword arguments declaration.
        self.db_conn = db_conn
        self.table_name = table_name
        self.label_column_name = label_column_name
        self.train_query = train_query
        self.validation_query = validation_query
        self.imputation_strategy = imputation_strategy

        # Constants declaration.
        self.source_data_schema_name = SOURCE_DATA_SCHEMA_NAME
        self.source_data_table_name = SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME
        self.modeling_schema_name = MODELING_SCHEMA_NAME
        self.role_name = ROLE_NAME

        # Parameters to be saved. This is returned by self.get_params()
        self.params = {"imputation_strategy": imputation_strategy}

    def fit(self, X, y, **kwargs):
        """Fit the baseline classifier. The `X` and `y` inputs are ignored.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Training feature(s).
            y (pd.DataFrame) -- array-like of shape (n_samples,).
                                Training label.
            kwargs (dict) -- expects a dictionary with `start_datetime_est` and `end_datetime_est` as keys.

        Returns:
            self : object
                Returns the instance itself.
        """

        # Set role if role_name is not None
        if self.role_name is not None:
            set_role(db_conn=self.db_conn, role_name=self.role_name)

        table_content = self.train_query.format(
            source_data_schema_name=self.source_data_schema_name,
            source_data_table_name=self.source_data_table_name,
            split_start_datetime=kwargs["start_datetime_est"],
            split_end_datetime=kwargs["end_datetime_est"],
        )
        logging.debug(f"This is `train_query`:\n{table_content}.")

        # Create table in database with table_content.
        create_table_with_sql_query(
            db_conn=self.db_conn,
            schema_name=self.modeling_schema_name,
            table_name=self.table_name,
            table_content=table_content,
        )
        logging.debug("Finished fitting the model.")

        return self

    def predict(self, X, **kwargs):
        """Return probability estimates for X at a threshold of 0.5.
        Predict `X` using results of the baseline computed in `self.fit()`.
        This looks up the `(center_key, termination_number)` of `X`
        from the `table_name` that has been fitted.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples).
                                     Returns the probability of the sample for each class in the
                                     model at a threshold of 0.5; 0 if result < 0.5, 1 otherwise.
        """

        table_content = self.validation_query.format(
            source_data_schema_name=self.source_data_schema_name,
            source_data_table_name=self.source_data_table_name,
            modeling_schema_name=self.modeling_schema_name,
            table_name=self.table_name,
            split_start_datetime=kwargs["start_datetime_est"],
            split_end_datetime=kwargs["end_datetime_est"],
        )

        logging.debug(f"This is `validation_query`:\n{table_content}.")

        results = pd.read_sql_query(
            table_content,
            self.db_conn,
        )

        results = fill_na(
            data=results,
            column_name=self.label_column_name,
            imputation_strategy=self.imputation_strategy,
        )

        prediction = results.loc[:, self.label_column_name].array
        prediction = (prediction > 0.5).astype(int)

        return prediction

    def predict_proba(self, X, **kwargs):
        """Return probability estimates for X.
        Predict `X` using results of the baseline computed in `self.fit()`.
        This looks up the `(center_key, termination_number)` of `X`
        from the `table_name` that has been fitted.

        Keyword arguments:
            X (pd.DataFrame) : array-like of shape (n_samples, n_features).
                Training data.
            kwargs (dict) -- dictionary with `start_datetime_est` and `end_datetime_est` as keys.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples, n_classes) or list of such arrays
                                    Returns the probability of the sample for each class in
                                    the model, with the positive class as the result of `train_query`.
                                    The classes are ordered arithmetically.
        """

        table_content = self.validation_query.format(
            source_data_schema_name=self.source_data_schema_name,
            source_data_table_name=self.source_data_table_name,
            modeling_schema_name=self.modeling_schema_name,
            table_name=self.table_name,
            split_start_datetime=kwargs["start_datetime_est"],
            split_end_datetime=kwargs["end_datetime_est"],
        )

        logging.debug(f"This is `validation_query`:\n{table_content}.")

        results = pd.read_sql_query(
            table_content,
            self.db_conn,
        )

        results = fill_na(
            data=results,
            column_name=self.label_column_name,
            imputation_strategy=self.imputation_strategy,
        )

        class_1 = results.loc[:, self.label_column_name].array.reshape(-1, 1)
        class_0 = 1 - class_1
        prediction = np.hstack((class_0, class_1))

        return prediction

    def get_params(self):
        """Return the parameters that will be saved."""
        return self.params


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Read json file containing split data dictionary.
    with open(SPLIT_FILE, "r") as f:
        splits = json.load(f)

    # Get database connection.
    db_conn = get_db_conn()

    config = modeling_config["model_grid"][
        "src.pipeline.call.model.baseline.AnswerRateAtCenter"
    ]["init_param"]

    init_param = {
        "db_conn": db_conn,
        "table_name": config["table_name"],
        "label_column_name": config["label_column_name"],
        "train_query": config["train_query"],
        "validation_query": config["validation_query"],
    }

    answer_rate_baseline = AnswerRateAtCenter(imputation_strategy="mean", **init_param)

    train_kwargs = {
        "start_datetime_est": splits[0]["train"]["start_datetime_est"],
        "end_datetime_est": splits[0]["train"]["end_datetime_est"],
    }

    validation_kwargs = {
        "start_datetime_est": splits[0]["validation"]["start_datetime_est"],
        "end_datetime_est": splits[0]["validation"]["end_datetime_est"],
    }
    answer_rate_baseline.fit(X=3, y=3, **train_kwargs)
    predictions = answer_rate_baseline.predict_proba(X=3, **validation_kwargs)
    print(predictions)


# main()
