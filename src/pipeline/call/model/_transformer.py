import logging

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils import check_array

# Check https://github.com/dssg/triage/blob/3ae02afce3beb1e054bebd49319811c3823d72c0/src/triage/component/catwalk/estimators/transformers.py


class CutOff(BaseEstimator, TransformerMixin):
    """Transform feature cutting values out of established range.
    It inherits `BaseEstimator` and `ClassifierMixin` from `sklearn.base`.

    Usage:

            from sklearn.pipeline import Pipeline

            minmax_scaler = preprocessing.MinMaxScaler()
            cutoff = CutOff()
            logistic_regression = linear_model.LogisticRegression()

            pipeline = Pipeline([
                ('minmax_scaler', minmax_scaler),
                ('cutoff', cutoff),
                ('logistic_regression', logistic_regression)
            ])

            pipeline.fit(X_train, y_train)
            pipeline.predict(X_test)
    """

    def __init__(self, feature_range=(0, 1), copy=True):
        """Initialize instance variables.

        Keyword arguments:
            feature_range (tuple) -- range of allowed values. Default=`(0, 1)`.
            copy (bool) -- whether a forced copy will be triggered (this is an input to `sklearn.utils.check_array`)
                            If copy=False, a copy might be triggered by a conversion.
        """

        self.feature_range = feature_range
        self.copy = copy

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        """Transform input variable `X`.

        Keyword arguments:
            X (np.array) -- Data to be transformed (n_rows, n_cols).

        Returns:
            X (np.array) -- Transformed data.
        """
        feature_range = self.feature_range

        X = check_array(X, copy=self.copy, ensure_2d=True)

        if np.any(X > feature_range[1]) or np.any(X < feature_range[0]):
            logging.warning(
                f"You got feature values that are out of the range: {feature_range}"
                f"The feature values will cutoff to fit the range {feature_range}."
            )

        X[X > feature_range[1]] = feature_range[1]
        X[X < feature_range[0]] = feature_range[0]

        return X


def main():
    """Main function to exemplify how to use the function."""
    cutoff = CutOff()
    # Initialize a sample array for testing purposes.
    x = np.array([[1, 0.8, 0.1, -0.1, 2, 0.6, 0.3, 4]])
    print("Actual output:", cutoff.transform(x))
    print("Expected output:")


# main()
