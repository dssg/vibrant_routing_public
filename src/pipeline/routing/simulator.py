import logging
import heapq
from datetime import timedelta
import time

import dateutil.parser
import joblib
import numpy as np
import pandas as pd
import yaml
from config.project_constants import MODELING_CONFIG_FILE, FEATURES_COLUMNS_TO_RENAME
from src.pipeline.routing.cohort_creator import cohort_creator
from src.pipeline.routing.split_data import split_data
from src.pipeline.routing.feature_creator import feature_creator
from src.pipeline.routing.matrix_creator import matrix_creator
from src.pipeline.routing.populate_simulation_table import PopulateSimulationTable
from src.pipeline.routing.predict import predict
from src.utils.sql_util import (
    create_table_with_sql_query,
    get_db_conn,
    get_wait_time_from_center,
    get_saved_model_info_from_db,
)


def simulate_routing(
    db_conn,
    model,
    routing_table_path,
    config_routing_level,
    config_feature,
    random_seed=None,
):
    """Simulate the routing of calls.

    Keyword arguments:
        db_conn (object) -- database connection.
        model (object) -- model used to predict whether a call will be picked up or not a given call center.
        routing_table_path (str) -- path where to find the routing table of interest.
        config_routing_level (dict) -- dictionary with the elements that characterise the routing level configuration.
        config_feature (dict) -- information about the features to be created.
        random_seed (int, optional) -- used to set the random state of the random number generator to ensure reproducibility.
                                       Defaults to NoneType.
    """

    if random_seed is not None:
        # Set simulation random seed for reproducibility.
        np.random.seed(random_seed)
    else:
        # Reset the initial random state.
        np.random.seed(None)
        # Get the random seed used for this experiment.
        random_seed = np.random.choice(np.random.get_state()[1][0])
        # Set the random seed for reproducibility.
        np.random.seed(random_seed)

    logging.debug(f"The random seed for this simulation is {random_seed}")

    # Load routing table to pd.DataFrame and set column names to lower case.
    routing_table = pd.read_csv(routing_table_path)
    routing_table.columns = routing_table.columns.str.lower()

    # Set the exchange code as index in the routing table to ease upcoming lookups.
    routing_table["npanxx"] = pd.to_numeric(routing_table["npanxx"])
    routing_table = routing_table.set_index("npanxx")

    # Replace NULL values with None.
    routing_table = routing_table.replace({np.nan: None})

    # Create dictionary with the terminology used in the routing table to
    # name the call centers per attempt number.
    center_key_map = {0: "center1id", 1: "center2id", 2: "center3id", 3: "center4id"}
    termination_number_map = {
        0: "center1termination",
        1: "center2termination",
        2: "center3termination",
        3: "center4termination",
    }

    # Load active calls data based on input from config routing level.
    active_calls_table_name = [
        table["name"]
        for table in config_routing_level["tables_to_create"]
        if table["tag"] == "future"
    ][0]
    active_calls = db_conn.execute(
        f"select * from {config_routing_level['database_config']['schema_name']}.{active_calls_table_name}"
    )
    active_calls = pd.DataFrame(active_calls)

    # Instantiate populate simulation table's class.
    simulated_routing_attempts_table = PopulateSimulationTable(
        db_conn=db_conn,
        schema_name=config_routing_level["database_config"]["schema_name"],
        table_name=config_routing_level["feature_config"][
            "simulated_routing_attempts_table_name"
        ],
    )

    # Setup the queue of calls. The priority will be based on the first element of the queue.
    # The elements of the queue will be:
    #   datetime [EST] of the call, call_key, exchange code (caller_npanxx),
    #   attempt number (initialized at 0), and total_ring_time_sec (initialized at 0).
    attempt_number = 0
    total_ring_time_sec = 0
    calls_queue = []
    for _, row in active_calls.iterrows():
        heapq.heappush(
            calls_queue,
            (
                dateutil.parser.parse(str(row["initiated_datetime_est"])),
                row["call_key"],
                row["caller_npanxx"],
                attempt_number,
                total_ring_time_sec,
            ),
        )

    logging.info(
        f"Calls simulation started. Initial number of calls to simulate: {len(active_calls)}",
    )
    start_time = time.time()
    while calls_queue:
        # Get next call in queue.
        next_call = heapq.heappop(calls_queue)
        logging.debug(f"Next call: {next_call}.")

        # Get the values necessary to know the call center where to route the call.
        call_arrived_datetime_est = next_call[0]
        call_key = next_call[1]
        exchange_code = next_call[2]
        attempt_number = next_call[3]
        total_ring_time_sec = next_call[4]

        # Get the center_key and termination_number where to route the call.
        center_key = routing_table.loc[exchange_code, center_key_map[attempt_number]]
        termination_number = routing_table.loc[
            exchange_code, termination_number_map[attempt_number]
        ]
        logging.debug(
            f"Exchange code {exchange_code} at attempt number {attempt_number+1}"
            f"will be routed to center_key: {center_key} and termination_number: {termination_number}."
            f"This call has been ringing for {total_ring_time_sec} seconds."
        )

        # Check if routing attempt is possible for this call.
        routing_attempt_is_possible = all(
            [
                len(center_key_map) >= attempt_number + 1,
                center_key is not None,
            ]
        )

        routing_attempt_id = {
            "call_key": call_key,
            "caller_npanxx": exchange_code,
            "arrived_datetime_est": str(call_arrived_datetime_est),
            "center_key": center_key,
            "termination_number": termination_number,
        }

        if routing_attempt_is_possible:
            # Get the attributes needed for feature computation for this call's routing attempt.
            routing_attempt_attributes_dict = (
                simulated_routing_attempts_table.get_routing_attempt_attributes(
                    routing_attempt_id=routing_attempt_id
                )
            )

            # Update the routing attempts attributes dictionary with the routing attempt id.
            routing_attempt_attributes_dict.update(routing_attempt_id)

            # Insert the current call's routing attempts attributes into the simulated routing attempts table.
            logging.debug(f"Inserting... {routing_attempt_attributes_dict}")
            simulated_routing_attempts_table.insert_data_into_table(
                data=routing_attempt_attributes_dict
            )

            # Create all the feature tables.
            feature_creator(
                db_conn=db_conn,
                source_data_schema_name=config_routing_level["database_config"][
                    "schema_name"
                ],
                source_data_table_name=config_routing_level["feature_config"][
                    "source_data_table_name"
                ],
                feature_schema_name=config_routing_level["feature_config"][
                    "feature_schema_name"
                ],
                feature_config_dict=config_feature,
                incoming_call_dict=routing_attempt_id,
                simulated_routing_attempts_table_name=config_routing_level[
                    "feature_config"
                ]["simulated_routing_attempts_table_name"],
            )
            # Create cohort table to later join the features tables.
            create_table_with_sql_query(
                db_conn=db_conn,
                schema_name=config_routing_level["database_config"]["schema_name"],
                table_name=config_routing_level["database_config"]["cohort_table_name"],
                table_content=config_routing_level["cohort_config"][
                    "cohort_query"
                ].format(
                    call_key=call_key,
                    center_key=center_key,
                    termination_number=termination_number,
                    arrived_datetime_est=call_arrived_datetime_est,
                ),
            )

            # Create a matrix with the cohort and the features.
            matrix = matrix_creator(
                db_conn=db_conn,
                schema_name=config_routing_level["database_config"]["schema_name"],
                database_config_dict=config_routing_level["database_config"],
                feature_config_dict=config_feature,
                matrix_config_dict=config_routing_level["matrix_creator_config"],
            )

            # Rename feature columns if needed.
            matrix.rename(columns=FEATURES_COLUMNS_TO_RENAME, inplace=True)

            # Compute the score of the call being picked up at the given call center.
            pick_up_score = predict(
                matrix=matrix,
                model=model,
                columns_to_remove=routing_attempt_id,
            )

            assert (
                pick_up_score <= 1.0 and pick_up_score >= 0.0
            ), f"The score (probability) of call being picked up"
            f"should be between 0 and 1, not {pick_up_score}"

            # Follow logic for the routing simulator.
            # Check if the call would be picked up.
            call_was_picked_up = np.random.binomial(n=1, p=pick_up_score)
            if call_was_picked_up:
                logging.debug(
                    "Call was picked up based on biased coin flip. "
                    f"Predicted score from model is: {pick_up_score}."
                )

                # Average ring_time_center and time_to_answer_center are not consistent.
                # When in doubt, choose time to time_to_answer_center.
                # Historically, average time_to_answer_center is lower than ring_time_center.
                (
                    time_to_leave_center,
                    time_to_answer_center,
                ) = simulated_routing_attempts_table.get_center_historical_disposition_estimate(
                    center_key=center_key,
                    termination_number=termination_number,
                    stats_of_interest=[
                        "answered_avg_time_to_leave",
                        "answered_avg_time_to_answer",
                    ],
                )

                ring_time_center = time_to_answer_center
                talk_time_center = time_to_leave_center - time_to_answer_center

                # 1 seconds was added in the original definition of this attribute. See `raw_to_processed.sql` file.
                datetime_to_disposition_est = call_arrived_datetime_est + timedelta(
                    seconds=ring_time_center + 1
                )
                datetime_to_leave_center_est = call_arrived_datetime_est + timedelta(
                    seconds=time_to_leave_center + 1
                )

                call_is_answered_in_state = int(
                    routing_attempt_attributes_dict["center_state_abbrev"]
                    == routing_attempt_attributes_dict["caller_state_abbrev"]
                )

                # Details of what happened to the call.
                incoming_call_disposition_dict = {
                    "arrived_datetime_est": str(call_arrived_datetime_est),
                    "completed_at_center": 1,
                    "answered_at_center": 1,
                    "flowout_from_center": 0,
                    "abandoned_at_center": 0,
                    "time_to_abandon_center": 0,
                    "ring_time_center": ring_time_center,
                    "time_to_answer_center": time_to_answer_center,
                    "talk_time_center": talk_time_center,
                    "time_to_leave_center": time_to_leave_center,
                    "attempt_number": attempt_number + 1,
                    "datetime_to_disposition_est": str(datetime_to_disposition_est),
                    "datetime_to_leave_center_est": str(datetime_to_leave_center_est),
                    "answered_in_state": call_is_answered_in_state,
                    "answered_out_state": 1 - call_is_answered_in_state,
                }
                logging.debug(
                    f"Updating... {routing_attempt_id} with {incoming_call_disposition_dict}",
                )

                # Update the simulated routing attempts table with the disposition call's data.
                simulated_routing_attempts_table.update_row_in_table(
                    data=incoming_call_disposition_dict,
                    row_identifier=routing_attempt_id,
                )

                logging.debug(
                    f"Updating the caller's max attempt number and initiated datetime est... {routing_attempt_id}",
                )
                # Get the initiated datetime given the time the call was completed and the
                # total time the call rung for.
                initiated_datetime_est = str(
                    simulated_routing_attempts_table.get_initiated_datetime(
                        completed_datetime=call_arrived_datetime_est,
                        total_ring_time_sec=total_ring_time_sec,
                        attempt_number=attempt_number,
                    )
                )
                # Get the initiated part of day for the call (in est).
                initiated_part_of_day = (
                    simulated_routing_attempts_table.get_part_of_day(
                        datetime_to_extract=initiated_datetime_est
                    )
                )
                # Update the max_attempt_num, initiated_datetime_est, and initiated_part_of_day
                # attribute of all the routing attempts for this caller.
                simulated_routing_attempts_table.update_rows_in_table(
                    data={
                        "max_attempt_num": attempt_number + 1,
                        "initiated_datetime_est": initiated_datetime_est,
                        "initiated_part_of_day": initiated_part_of_day,
                    },
                    row_identifier={"call_key": call_key},
                )
            else:
                logging.debug(
                    "Call was not picked up based on biased coin flip. "
                    f"Predicted score from model is: {pick_up_score}."
                )
                wait_time_at_center_minute = get_wait_time_from_center(
                    db_conn=db_conn,
                    center_key=center_key,
                    termination_number=termination_number,
                )
                wait_time_at_center_sec = wait_time_at_center_minute * 60
                logging.debug(
                    f"The call waited for {wait_time_at_center_minute} minute(s) at {center_key} call center."
                )

                current_wait_minute = int(total_ring_time_sec / 60)
                # Check if the call was abandoned.
                proba_abandonment = (
                    simulated_routing_attempts_table.get_probability_abandonment(
                        current_wait_minute=current_wait_minute,
                        add1_wait_minute=wait_time_at_center_minute,
                    )
                )

                # Update the total ring time of the caller.
                total_ring_time_sec += wait_time_at_center_sec

                # Apply logic based on whether the call was abandoned or not.
                call_was_abandoned = np.random.binomial(n=1, p=proba_abandonment)
                if call_was_abandoned:
                    logging.debug(
                        "Call was abandoned based on biased coin flip. "
                        "Estimated probability of abandonment based on the "
                        f"current wait time of the caller: {current_wait_minute} minutes, "
                        f"is {proba_abandonment}."
                    )

                    # Assume that call rung for 4 seconds before leaving the center.
                    # This behavior was observed from historical data.
                    time_to_leave_center = wait_time_at_center_sec + 4
                    ring_time_center = time_to_leave_center
                    # 1 seconds was added in the original definition of this attribute. See `raw_to_processed.sql` file.
                    datetime_to_disposition_est = call_arrived_datetime_est + timedelta(
                        seconds=ring_time_center + 1
                    )
                    datetime_to_leave_center_est = (
                        call_arrived_datetime_est
                        + timedelta(seconds=time_to_leave_center + 1)
                    )

                    # Details of what happened to the call.
                    incoming_call_disposition_dict = {
                        "arrived_datetime_est": str(call_arrived_datetime_est),
                        "completed_at_center": 1,
                        "answered_at_center": 0,
                        "flowout_from_center": 0,
                        "abandoned_at_center": 1,
                        "time_to_abandon_center": wait_time_at_center_sec,
                        "ring_time_center": ring_time_center,
                        "time_to_answer_center": 0,
                        "talk_time_center": 0,
                        "time_to_leave_center": time_to_leave_center,
                        "attempt_number": attempt_number + 1,
                        "datetime_to_disposition_est": str(datetime_to_disposition_est),
                        "datetime_to_leave_center_est": str(
                            datetime_to_leave_center_est
                        ),
                        "answered_in_state": 0,
                        "answered_out_state": 0,
                    }

                    logging.debug(
                        f"Updating... {routing_attempt_id} with {incoming_call_disposition_dict}",
                    )
                    # Update the simulated routing attempts table with the disposition call's data.
                    simulated_routing_attempts_table.update_row_in_table(
                        data=incoming_call_disposition_dict,
                        row_identifier=routing_attempt_id,
                    )

                    logging.debug(
                        f"Updating the caller's max attempt number and initiated datetime est... {routing_attempt_id}",
                    )
                    # Get the initiated datetime given the time the call was completed and the
                    # total time the call rung for.
                    initiated_datetime_est = str(
                        simulated_routing_attempts_table.get_initiated_datetime(
                            completed_datetime=call_arrived_datetime_est,
                            total_ring_time_sec=total_ring_time_sec,
                            attempt_number=attempt_number,
                        )
                    )
                    # Get the initiated part of day for the call (in est).
                    initiated_part_of_day = (
                        simulated_routing_attempts_table.get_part_of_day(
                            datetime_to_extract=initiated_datetime_est
                        )
                    )
                    # Update the max_attempt_num, initiated_datetime_est, and initiated_part_of_day
                    # attribute of all the routing attempts for this caller.
                    simulated_routing_attempts_table.update_rows_in_table(
                        data={
                            "max_attempt_num": attempt_number + 1,
                            "initiated_datetime_est": initiated_datetime_est,
                            "initiated_part_of_day": initiated_part_of_day,
                        },
                        row_identifier={"call_key": call_key},
                    )
                else:
                    logging.debug(
                        "Call was not abandoned based on biased coin flip. "
                        "Estimated probability of abandonment based on the "
                        f"current wait time of the caller: {current_wait_minute} minutes, "
                        f"is {proba_abandonment}."
                    )
                    logging.info(f"This call will be routed again.")

                    heapq.heappush(
                        calls_queue,
                        (
                            dateutil.parser.parse(
                                (
                                    call_arrived_datetime_est
                                    + timedelta(seconds=wait_time_at_center_sec)
                                ).strftime("%Y-%m-%d %H:%M:%S")
                            ),
                            call_key,
                            exchange_code,
                            attempt_number + 1,
                            total_ring_time_sec,
                        ),
                    )
                    # Time to leave center and ring time.
                    time_to_leave_center = wait_time_at_center_sec
                    ring_time_center = time_to_leave_center
                    # 1 seconds was added in the original definition of this attribute. See `raw_to_processed.sql` file.
                    datetime_to_disposition_est = call_arrived_datetime_est + timedelta(
                        seconds=ring_time_center + 1
                    )
                    datetime_to_leave_center_est = (
                        call_arrived_datetime_est
                        + timedelta(seconds=time_to_leave_center + 1)
                    )

                    # Details of what happened to the call.
                    incoming_call_disposition_dict = {
                        "arrived_datetime_est": str(call_arrived_datetime_est),
                        "completed_at_center": 0,
                        "answered_at_center": 0,
                        "flowout_from_center": 1,
                        "abandoned_at_center": 0,
                        "time_to_abandon_center": 0,
                        "ring_time_center": ring_time_center,
                        "time_to_answer_center": 0,
                        "talk_time_center": 0,
                        "time_to_leave_center": time_to_leave_center,
                        "attempt_number": attempt_number + 1,
                        "datetime_to_disposition_est": str(datetime_to_disposition_est),
                        "datetime_to_leave_center_est": str(
                            datetime_to_leave_center_est
                        ),
                        "answered_in_state": 0,
                        "answered_out_state": 0,
                    }

                    logging.debug(
                        f"Updating... {routing_attempt_id} with {incoming_call_disposition_dict}",
                    )
                    # Update the simulated routing attempts table with the disposition call's data.
                    simulated_routing_attempts_table.update_row_in_table(
                        data=incoming_call_disposition_dict,
                        row_identifier=routing_attempt_id,
                    )
        else:
            # If routing attempt is not possible, we assume that the
            # call flowed out to the national backup network.
            # However, we don't know what happened to the call.
            logging.info("This is the backup network.")

            # Get the attributes needed for feature computation for this call's routing attempt.
            routing_attempt_attributes_dict = (
                simulated_routing_attempts_table.get_routing_attempt_attributes(
                    routing_attempt_id=routing_attempt_id
                )
            )

            # Sql does not understand None. "NULL" also does not work.
            routing_attempt_id.update(
                {"center_key": "National Backup", "termination_number": -1}
            )

            # Update the routing attempts attributes dictionary with the routing attempt id.
            routing_attempt_attributes_dict.update(routing_attempt_id)

            # Details about what happened to this call.
            incoming_call_disposition_dict = {
                "arrived_datetime_est": str(call_arrived_datetime_est),
                "completed_at_center": 0,
                "answered_at_center": 0,
                "flowout_from_center": 0,
                "abandoned_at_center": 0,
                "time_to_abandon_center": 0,
                "attempt_number": attempt_number + 1,
            }

            # Update the routing attempts attributes dictionary with the call disposition data.
            routing_attempt_attributes_dict.update(incoming_call_disposition_dict)

            logging.debug(
                f"Inserting... with {routing_attempt_attributes_dict}",
            )

            # Insert routing attempt attributes and disposition data for the call
            # to the simulated routing attempts table.
            simulated_routing_attempts_table.insert_data_into_table(
                data=routing_attempt_attributes_dict
            )

            logging.debug(
                f"Updating the caller's max attempt number and initiated datetime est... {routing_attempt_id}",
            )

            # Get the initiated datetime given the time the call was completed and the
            # total time the call rung for.
            initiated_datetime_est = str(
                simulated_routing_attempts_table.get_initiated_datetime(
                    completed_datetime=call_arrived_datetime_est,
                    total_ring_time_sec=total_ring_time_sec,
                    attempt_number=attempt_number,
                )
            )
            # Get the initiated part of day for the call (in est).
            initiated_part_of_day = simulated_routing_attempts_table.get_part_of_day(
                datetime_to_extract=initiated_datetime_est
            )
            # Update the max_attempt_num, initiated_datetime_est, and initiated_part_of_day
            # attribute of all the routing attempts for this caller.
            simulated_routing_attempts_table.update_rows_in_table(
                data={
                    "max_attempt_num": attempt_number + 1,
                    "initiated_datetime_est": initiated_datetime_est,
                    "initiated_part_of_day": initiated_part_of_day,
                },
                row_identifier={"call_key": call_key},
            )

        logging.info("End of call attempt lifecycle.")

    # End the timer.
    end_time = time.time() - start_time
    logging.info(f"Calls simulation ended. Total elapsed time: {end_time} seconds")
    return random_seed


def main():
    """Example function to show the functioning of the routing simulator."""

    print("#" * 1, "Execution of the simulator pipeline started.")
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    # Get database connection.
    db_conn = get_db_conn()

    print("#" * 1, "Split datetime.")
    # Create split datetime
    split_datetime = split_data(
        temporal_config=modeling_config["routing_level_config"]["temporal_config"],
        output_filename=None,
    )

    print("#" * 1, "Create cohort")
    cohort_creator(
        db_conn=get_db_conn(),
        split_datetime=split_datetime,
        config=modeling_config["routing_level_config"],
    )

    # Retrieve the best model_id and model_path from db.
    model_id = modeling_config["routing_level_config"]["best_model_config"]["model_id"]
    best_model_path = get_saved_model_info_from_db(
        db_conn=db_conn, model_id=model_id, info_to_get="pickle_path"
    )[0]

    print("#" * 1, "Load best model.")
    # Load the best model from from model_path.
    model = joblib.load(best_model_path)

    # Path to routing table.
    routing_table_path = "/mnt/data/projects/vibrant-routing/data/20220604/vibrant_RoutingTable_202206031725.csv"

    # Create test config routing level.
    simulate_routing(
        db_conn=db_conn,
        model=model,
        routing_table_path=routing_table_path,
        config_routing_level=modeling_config["routing_level_config"],
        config_feature=modeling_config["feature_config"],
        random_seed=None,
    )


# main()
