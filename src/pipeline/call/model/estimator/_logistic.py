import yaml
import json
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler

from src.pipeline.call.model._transformer import CutOff
from config.project_constants import MODELING_CONFIG_FILE


# Check https://github.com/dssg/triage/blob/3ae02afce3beb1e054bebd49319811c3823d72c0/src/triage/component/catwalk/estimators/classifiers.py


class ScaledLogisticRegression(BaseEstimator, ClassifierMixin):
    """An in-place replacement for the scikit-learn's LogisticRegression.
    It inherits `BaseEstimator` and `ClassifierMixin` from `sklearn.base` and
    incorporates the MaxMinScaler, and the CutOff as preparations
    for the logistic regression.

    Takes in keyword arguments for `LogisticRegression` model.
    """

    def __init__(self, **kwargs):
        self.minmax_scaler = MinMaxScaler()
        self.cutoff = CutOff()
        self.logistic_regression = LogisticRegression(**kwargs)

        self.pipeline = Pipeline(
            [
                ("minmax_scaler", self.minmax_scaler),
                ("cutoff", self.cutoff),
                ("logistic_regression", self.logistic_regression),
            ]
        )

    def fit(self, X, y, **kwargs):
        """Fit the `pipeline` on input data and label.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Training feature(s).
            y (pd.DataFrame) -- array-like of shape (n_samples,).
                                Training label.

        Returns:
            self : object
                Returns the instance itself.
        """
        self.pipeline.fit(X, y, **kwargs)

        self.min_ = self.pipeline.named_steps["minmax_scaler"].min_
        self.scale_ = self.pipeline.named_steps["minmax_scaler"].scale_
        self.data_min_ = self.pipeline.named_steps["minmax_scaler"].data_min_
        self.data_max_ = self.pipeline.named_steps["minmax_scaler"].data_max_
        self.data_range_ = self.pipeline.named_steps["minmax_scaler"].data_range_

        self.coef_ = self.pipeline.named_steps["logistic_regression"].coef_
        self.intercept_ = self.pipeline.named_steps["logistic_regression"].intercept_

        self.classes_ = self.pipeline.named_steps["logistic_regression"].classes_

        return self

    def predict_proba(self, X, **kwargs):
        """Predict probability estimates.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.

        Returns:
            prediction (np.array) -- array-like of shape (n_samples).
                                     Returns the probability of the sample
                                     for each class in the model.
        """
        return self.pipeline.predict_proba(X, **kwargs)

    def predict_log_proba(self, X, **kwargs):
        """Predict logarithm of probability estimates.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples).
                                     Returns the log probability of the
                                     sample for each class in the model.
        """
        return self.pipeline.predict_log_proba(X, **kwargs)

    def predict(self, X, **kwargs):
        """Predict probability estimates at a threshold of 0.5.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples).
                                     Returns the probability of the sample for each class in the
                                     model at a threshold of 0.5; 0 if result < 0.5, 1 otherwise.
        """
        return self.pipeline.predict(X, **kwargs)

    def score(self, X, y, **kwargs):
        """Score using the `scoring` option on the given test data and labels.
        Defaults to `accuracy`.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.
            y (np.array) --   array-like of shape (n_samples,)
                             True labels for X.

        Returns:
            score (np.array) -- array-like of shape (n_samples).
                                Accuracy score with respect to
                                the true label.
        """
        return self.pipeline.score(X, y, **kwargs)

    def get_params(self, **kwargs):
        """Return the parameters that will be saved."""
        return self.pipeline.named_steps["logistic_regression"].get_params(**kwargs)


def main():
    """Main function to exemplify how to use the function."""
    # The following functions are imported here to avoid its import
    # by default when importing the _logistic script.
    from src.pipeline.call.matrix_creator import matrix_creator
    from src.utils.pipeline_util import split_features_label
    from src.utils.sql_util import get_db_conn

    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Read json file containing split data dictionary.
    with open("config/splits.json", "r") as f:
        splits = json.load(f)

    # Get database connection.
    db_conn = get_db_conn()

    kwargs = {"penalty": "l2", "C": 1.0}

    model = ScaledLogisticRegression(**kwargs)

    _, train_matrix = matrix_creator(
        db_conn=db_conn,
        split=splits[0]["train"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )
    _, validation_matrix = matrix_creator(
        db_conn=db_conn,
        split=splits[0]["validation"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )
    # Get the feature and label splits.
    train_X, train_y = split_features_label(
        matrix=train_matrix,
        label_column_name="answered_at_center",
        columns_to_remove=["routing_attempts_id", "initiated_datetime_est"],
    )

    validation_X, validation_y = split_features_label(
        matrix=validation_matrix,
        label_column_name="answered_at_center",
        columns_to_remove=["routing_attempts_id", "initiated_datetime_est"],
    )

    model.fit(X=train_X, y=train_y)
    predictions = model.predict_proba(X=validation_X)
    print(predictions)


# main()
