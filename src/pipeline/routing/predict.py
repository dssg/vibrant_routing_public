import joblib
import yaml
from config.project_constants import MODELING_CONFIG_FILE
from src.pipeline.routing.matrix_creator import matrix_creator
from src.utils.pipeline_util import split_features_label
from src.utils.sql_util import (
    get_db_conn,
    get_topk_models,
    get_saved_model_info_from_db,
)


def predict(
    matrix,
    model,
    columns_to_remove=None,
    pos_label=1,
):
    """Predict on the test data based on the trained model

    Keyword arguments:
        db_conn (object) -- database connection.
        matrix (pd.DataFrame) -- single datapoint containing only features.
        model (object) -- trained model.
        columns_to_remove (list[str]) -- list of columns to remove from dataset matrix for training.
                                      Defaults to NoneType.
        pos_label (int) -- the label of the positive class.

    Returns:
        y_pred (float) -- predicted probability.
    """

    X, _ = split_features_label(
        matrix=matrix,
        columns_to_remove=columns_to_remove,
    )

    y_pred = model.predict_proba(X)[:, pos_label]
    return y_pred[0]


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Get database connection.
    db_conn = get_db_conn()

    columns_to_remove = [
        "call_key",
        "arrived_datetime_est",
        "center_key",
        "termination_number",
    ]

    matrix = matrix_creator(
        db_conn=db_conn,
        schema_name=modeling_config["routing_level_config"]["database_config"][
            "schema_name"
        ],
        database_config_dict=modeling_config["routing_level_config"]["database_config"],
        feature_config_dict=modeling_config["feature_config"],
        matrix_config_dict=modeling_config["routing_level_config"][
            "matrix_creator_config"
        ],
    )

    model_id = get_topk_models(db_conn=db_conn).model_id[0]
    best_model_path = get_saved_model_info_from_db(
        db_conn=db_conn, model_id=model_id, info_to_get="pickle_path"
    )[0]

    model = joblib.load(best_model_path)

    prediction = predict(
        matrix=matrix,
        model=model,
        columns_to_remove=columns_to_remove,
    )
    print(prediction)


# main()
