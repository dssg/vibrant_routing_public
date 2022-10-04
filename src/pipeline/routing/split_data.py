import json
import logging
from datetime import datetime, timedelta

import yaml
from config.project_constants import (
    EARLIEST_INITIATED_DATETIME_EST,
    LATEST_INITIATED_DATETIME_EST,
    MODELING_CONFIG_FILE,
    ROUTING_LEVEL_SPLIT_FILE,
)


def split_data(temporal_config, output_filename=None):
    """Output a dictionary of time splits (EST) that will be used to create the active call queue
    and historical datasets for the routing simulation.

    Keyword arguments:
        temporal_config (dict) -- dictionary with parameters to perform the temporal configuration.
        output_filename (str, optional) -- file name for the json file that contains the time splits.
                                            Defaults to NoneType.

    Raises:
        ValueError -- if `simulation_start_datetime` from `temporal_config` dict is `NoneType`.

    Returns:
        split_datetimes (dict) -- dictionary with time splits to create the active call queue and historical datasets.
                                  Example of the output:
                                    {
                                        "historical": {
                                            "start_datetime_est": "2021-12-01 00:00:00",
                                            "end_datetime_est": "2022-05-25 23:59:59.999999"
                                        },
                                        "simulation": {
                                            "start_datetime_est": "2022-05-26 00:00:00",
                                            "end_datetime_est": "2022-05-27 00:00:00"
                                        }
                                    }
    """
    # Raise error if simulation start_datetime is None.
    if temporal_config["simulation_start_datetime"] is None:
        logging.error("Simulation start datetime can not be None.")
        raise ValueError("Simulation start datetime can not be None.")

    # Format simulation start_datetime.
    simulation_start_datetime = datetime.strptime(
        temporal_config["simulation_start_datetime"], temporal_config["time_format"]
    )

    try:
        simulation_duration_period, simulation_duration_period_slot = temporal_config[
            "simulation_duration"
        ].split(" ")
    except:
        simulation_duration_period = temporal_config["simulation_duration"]
        simulation_duration_period_slot = ""

    # Calculate simulation end_datetime.
    if simulation_duration_period_slot is None:
        simulation_end_datetime = datetime.strptime(
            LATEST_INITIATED_DATETIME_EST, temporal_config["time_format"]
        )
    elif simulation_duration_period_slot.startswith("micro"):
        simulation_end_datetime = simulation_start_datetime + timedelta(
            microseconds=int(simulation_duration_period)
        )
    elif simulation_duration_period_slot.startswith("sec"):
        simulation_end_datetime = simulation_start_datetime + timedelta(
            seconds=int(simulation_duration_period)
        )
    elif simulation_duration_period_slot.startswith("min"):
        simulation_end_datetime = simulation_start_datetime + timedelta(
            minutes=int(simulation_duration_period)
        )
    elif simulation_duration_period_slot.startswith("h"):
        simulation_end_datetime = simulation_start_datetime + timedelta(
            hours=int(simulation_duration_period)
        )
    elif simulation_duration_period_slot.startswith("day"):
        simulation_end_datetime = simulation_start_datetime + timedelta(
            days=int(simulation_duration_period)
        )
    else:
        # Default to days.
        simulation_end_datetime = simulation_start_datetime + timedelta(
            days=int(simulation_duration_period)
        )

    # Format historical start_datetime.
    if temporal_config["historical_start_datetime"] is None:
        historical_start_datetime = datetime.strptime(
            EARLIEST_INITIATED_DATETIME_EST, temporal_config["time_format"]
        )
    else:
        historical_start_datetime = datetime.strptime(
            temporal_config["historical_start_datetime"],
            temporal_config["time_format"],
        )

    # Calculate historical end_datetime:
    # subtract one microseconds to guarantee disjoint data set.
    historical_end_datetime = simulation_start_datetime - timedelta(microseconds=1)

    # Simulation datetime dictionary.
    simulation_datetime = {
        "start_datetime_est": str(simulation_start_datetime),
        "end_datetime_est": str(simulation_end_datetime),
    }

    # Historical datetime dictionary.
    historical_datetime = {
        "start_datetime_est": str(historical_start_datetime),
        "end_datetime_est": str(historical_end_datetime),
    }

    split_datetime = {
        "historical": historical_datetime,
        "simulation": simulation_datetime,
    }

    logging.debug(f"temporal_datetimes: \n{split_datetime}")

    if output_filename is not None:
        with open(output_filename, "w") as split_data_file:
            split_data_file.write(json.dumps(split_datetime, indent=4))

    return split_datetime


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    split_datetime = split_data(
        temporal_config=modeling_config["routing_level_config"]["temporal_config"],
        output_filename=ROUTING_LEVEL_SPLIT_FILE,
    )

    print(split_datetime)


# main()
