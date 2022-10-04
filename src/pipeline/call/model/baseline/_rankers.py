import json
import logging

import pandas as pd
import yaml
from config.project_constants import MODELING_CONFIG_FILE, SPLIT_FILE
from src.pipeline.call.matrix_creator import matrix_creator
from src.utils.sql_util import get_db_conn

# `import * from _rankers` will import the modules
# that are defined only in this list.
__all__ = ["FeatureRanker"]


class FeatureRanker(object):
    def __init__(self, name_of_feature_to_rank, low_value_is_ranked_higher=True):
        """Baseline that ranks the values of a given feature.
        Given a feature created by `feature_creator` function, this
        baseline simply takes the values of the feature, and compute a
        ranking of the values in percentile form as `percentile_ranked_score`.
        The `low_value_is_ranked_higher` informs this class on how to handle the computed
        percentile_ranked_score for the feature. If `low_value_is_ranked_higher` is True,
        then the positive class is calculated as `1-percentile_ranked_score` and ranked higher.

        Keyword arguments:
            name_of_feature_to_rank (str) --  The `name_of_feature_to_rank` is the name of the feature of interest.
                                               It must be a valid feature name i.e the `name_of_feature_to_rank`
                                               must exists in the training matrix.This is validated in the
                                               `self.fit()` method and fails if the `name_of_feature_to_rank` is invalid.
            low_value_is_ranked_higher (bool, optional) --  The `low_value_is_ranked_higher` is used to inform the class method
                                                            on how to compute the positive and negative class. If `low_value_is_ranked_higher`
                                                            is True, the feature that is expected to perform better should have a higher
                                                            rank and vice-versa. For example, for the `number_of_calls_at_center_in_the_past_x_mins`
                                                            feature, `low_value_is_ranked_higher` can be set to True if fewer calls implies
                                                            higher likelihood of a call being picked up at a call center.
                                                            Defaults to `True`.
        """

        self.name_of_feature_to_rank = name_of_feature_to_rank
        self.low_value_is_ranked_higher = low_value_is_ranked_higher

    def fit(self, X, y, **kwargs):
        """Dummy fit method.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Training feature(s).
            y (pd.DataFrame) -- array-like of shape (n_samples,).
                                Training label.
        Returns:
            self (object) -- the instance itself.
        """

        # Validate that the features of the training data includes the `name_of_feature_to_rank`.
        if self.name_of_feature_to_rank not in X.columns:
            logging.error(
                f"{self.name_of_feature_to_rank} is not a valid feature name!"
            )
            raise ValueError(
                f"{self.name_of_feature_to_rank} is not a valid feature name!"
            )

        return self

    def predict(self, X, **kwargs):
        """Return precentile ranked score for X at a threshold of 0.5.

        Keyword arguments:
            X (pd.DataFrame) -- array-like of shape (n_samples, n_features).
                                Data to be predicted.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples).
                                     Returns the percentile ranked score of the sample for each class in the
                                     model at a threshold of 0.5; 0 if percentile_ranked_score < 0.5, 1 otherwise.
        """
        percentile_ranked_score = self.predict_proba(X, **kwargs)[:, 1]
        prediction = (percentile_ranked_score > 0.5).astype(int)

        return prediction

    def predict_proba(self, X, **kwargs):
        """Return percentile ranked score for X for each classes.
        Keyword arguments:
            X (pd.DataFrame) : array-like of shape (n_samples, n_features).
                               Training data.

        Returns:
            prediction (np.array) -- ndarray of shape (n_samples, n_classes) or list of such arrays
                                    Returns the percentile ranked score of the sample for each class in the model.
                                    The classes are ordered arithmetically. The positive class is determined by
                                    the specified low_value_is_ranked_higher.
        """
        percentile_ranked_score = pd.DataFrame(X[self.name_of_feature_to_rank])
        percentile_ranked_score["ranked_score"] = percentile_ranked_score.rank(pct=True)

        if self.low_value_is_ranked_higher:
            # Inverse the result if the low value from that feature should be ranked higher:
            # this is important for the downstream evaluation modules
            # like AUC-ROC that sorts the predictions and assumes
            # that higher score corresponds to higher likelihood of the class.
            percentile_ranked_score["class_0"] = percentile_ranked_score["ranked_score"]
            percentile_ranked_score["class_1"] = (
                1 - percentile_ranked_score["ranked_score"]
            )
        else:
            percentile_ranked_score["class_0"] = (
                1 - percentile_ranked_score["ranked_score"]
            )
            percentile_ranked_score["class_1"] = percentile_ranked_score["ranked_score"]

        prediction = percentile_ranked_score[["class_0", "class_1"]].values

        return prediction

    def get_params(self):
        """Return the parameters that will be saved."""

        return self.__dict__


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # For testing purposes: read splits.json.
    with open(SPLIT_FILE) as f:
        splits = json.load(f)

    matrix_file_path, validation_matrix = matrix_creator(
        db_conn=get_db_conn(),
        split=splits[0]["validation"],
        schema_name=modeling_config["database_config"]["modeling_schema_name"],
        database_config_dict=modeling_config["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["matrix_creator_config"],
        matrix_folder_path=modeling_config["matrix_folder_path"],
    )

    print(validation_matrix.columns[-1])
    feature_to_rank = {
        "name_of_feature_to_rank": validation_matrix.columns[-1],
        "low_value_is_ranked_higher": False,
    }
    feature_ranker = FeatureRanker(**feature_to_rank)
    predictions = feature_ranker.predict_proba(validation_matrix)
    print(predictions)
    print(feature_ranker.predict(validation_matrix))
    print(feature_ranker.get_params())


# main()
