import json
from datetime import datetime, timedelta

import click
import yaml
from config.project_constants import MODELING_CONFIG_FILE


def split_data(temporal_config, output_filename=None):
    """Output a list of dictionaries with the time splits (EST) to create the train and validation datasets.
    The time splits are created assuming inclusive intervals (e.g., train dataset = [train start, train end]).
    If required, the outputted list will be written into a json file.

    Keyword arguments:
        temporal_config (dict) -- dictionary with the parameters to perform the temporal configuration.
        output_filename (str) -- file name for the json file that contains the time splits to create the train and validation datasets.
                                 Defaults to `None`.

    Returns:
        splits (list) -- list of dictionaries with the time splits to create the train and validation datasets.
                         Example of the output:
                            [
                                {
                                    "train": {
                                        "start_datetime_est": "2019-10-17 23:59:59",
                                        "end_datetime_est": "2020-01-15 23:59:58.999999"
                                    },
                                    "validation": {
                                        "start_datetime_est": "2020-01-15 23:59:59",
                                        "end_datetime_est": "2020-02-14 23:59:59"
                                    }
                                }
                                , ...
                            ]
    """
    # Read the temporal configuration from temporal_config
    feature_end_time = datetime.strptime(
        temporal_config["feature_end_time"], temporal_config["time_format"]
    )
    feature_start_time = datetime.strptime(
        temporal_config["feature_start_time"], temporal_config["time_format"]
    )
    validation_duration = int(temporal_config["validation_duration_days"])
    training_duration = int(temporal_config["max_training_histories_days"])
    split_duration = validation_duration + training_duration
    update_frequency = int(temporal_config["update_frequency_days"])

    # Create list of paired train / validation sets of dates
    splits = []

    # Calculate the ends of the first split
    split_end = feature_end_time
    split_start = feature_end_time - timedelta(days=split_duration)

    # Create time splits while the ends of the split are inside the given time windows
    while split_start > feature_start_time:
        split_dict = {
            "train": {
                "start_datetime_est": str(split_start),
                "end_datetime_est": str(
                    split_end
                    - timedelta(
                        days=validation_duration, microseconds=1
                    )  # One microsecond is substracted to guarantee the disjoint of data sets.
                ),
            },
            "validation": {
                "start_datetime_est": str(
                    split_end - timedelta(days=validation_duration)
                ),
                "end_datetime_est": str(split_end),
            },
        }

        # Append split data to the list of splits
        splits.append(split_dict)

        # Calculate the ends of the following split
        split_end = split_end - timedelta(days=update_frequency)
        split_start = split_end - timedelta(days=split_duration)

    # Write output list in file if indicated in the function's parameters
    if output_filename is not None:
        with open(output_filename, "w") as split_data_file:
            split_data_file.write(json.dumps(splits, indent=4))

    return splits


@click.command()
@click.option(
    "--splits",
    prompt="Path to the .json file containing training/validation splits info",
    default="config/splits.json",
)
def main(splits):
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    split_data(
        temporal_config=modeling_config["temporal_config"],
        output_filename=splits,
    )


# main()
