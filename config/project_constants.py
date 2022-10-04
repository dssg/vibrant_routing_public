import os

# DB configuration.
DB_NAME = "vibrant-routing"
ROLE_NAME = "vibrant-routing-role"

# Source data configuration.
SOURCE_DATA_SCHEMA_NAME = "processed"
SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME = "routing_attempts"
SOURCE_DATA_ROUTING_LOGIC_TABLE_NAME = "routing_table"

# Create indexes on column
LABELS_TABLE_INDEX = "routing_attempts_id"
FEATURES_TABLE_INDEX = "routing_attempts_id"

# Dataset statistics.
EARLIEST_INITIATED_DATETIME_EST = "2019-07-01 00:00:14"
LATEST_INITIATED_DATETIME_EST = "2022-06-03 16:15:25"
EARLIEST_ARRIVED_DATETIME_EST = "2019-07-01 00:00:56"
LATEST_ARRIVED_DATETIME_EST = "2022-06-03 16:16:08"

# Defaults to `production` if the `PROJECT_ENVIRONMENT`` is not set.
project_environment = os.getenv("PROJECT_ENVIRONMENT") or "production"

if project_environment.startswith("dev"):
    print("Project constants: In development environment.")
    # Pre-computed features configuration.
    PRE_COMPUTED_FEATURES_CONFIG_FILE = "config/dev_pre_computed_features_config.yaml"
    PRE_COMPUTED_FEATURES_SCHEMA_NAME = "dev_pre_computed_features"
    PRE_COMPUTED_FEATURES_TABLE_NAME = "dev_pre_computed"
    PRE_COMPUTED_FEATURES_WITH_COHORT_TABLE_NAME = "pre_computed"

    # Call and routing level configuration.
    MODELING_SCHEMA_NAME = "dev_modeling"
    ROUTING_LEVEL_SCHEMA_NAME = "dev_routing_level"
    ROUTING_LEVEL_SPLIT_FILE = "config/dev_routing_level_split_datetime.json"

    # Experiment configuration.
    EXPERIMENT_SCHEMA_NAME = "dev_experiments"
    EXPERIMENT_SCHEMA_NAME_ROUTING = "dev_experiments_routing"
    MODELING_CONFIG_FILE = "config/dev_modeling_config.yaml"
    SPLIT_FILE = "config/dev_splits.json"

    FEATURES_COLUMNS_TO_RENAME = {}
elif project_environment.startswith("prod"):
    print("Project constants: In production environment.")
    # Pre-computed features configuration.
    PRE_COMPUTED_FEATURES_CONFIG_FILE = "config/pre_computed_features_config.yaml"
    PRE_COMPUTED_FEATURES_SCHEMA_NAME = "pre_computed_features"
    PRE_COMPUTED_FEATURES_TABLE_NAME = "pre_computed"
    PRE_COMPUTED_FEATURES_WITH_COHORT_TABLE_NAME = "pre_computed"

    # Call and routing level configuration.
    MODELING_SCHEMA_NAME = "modeling"
    ROUTING_LEVEL_SCHEMA_NAME = "routing_level"
    ROUTING_LEVEL_SPLIT_FILE = "config/routing_level_split_datetime.json"

    # Experiment configuration.
    EXPERIMENT_SCHEMA_NAME = "experiments"
    EXPERIMENT_SCHEMA_NAME_ROUTING = "experiments_routing"
    MODELING_CONFIG_FILE = "config/modeling_config.yaml"
    SPLIT_FILE = "config/splits.json"

    #  TEMPORARY -> this should be empty once the models are retrained.
    # This is also used to rename the matrix columns in simulation.py
    # because the call level's features config file was changed after
    # some models had already been trained.
    FEATURES_COLUMNS_TO_RENAME = {
        "arrived_datetime_est_hour_numeric": "arrived_hour_of_day_numeric",
        "arrived_datetime_est_dow_numeric": "arrived_day_of_week_numeric",
        "arrived_datetime_est_day_numeric": "arrived_day_of_month_numeric",
        "arrived_datetime_est_doy_numeric": "arrived_day_of_year_numeric",
        "arrived_datetime_est_week_numeric": "arrived_week_of_year_numerics",
        "arrived_part_of_day_in_afternoon": "arrived_in_afternoon",
        "arrived_part_of_day_in_evening": "arrived_in_evening",
        "arrived_part_of_day_in_morning": "arrived_in_morning",
        "arrived_part_of_day_in_night": "arrived_in_night",
    }
else:
    raise ValueError(
        f"`{project_environment}` project environment is not recognized. Should be `prod(uction)` or `dev(elopment)`"
    )
