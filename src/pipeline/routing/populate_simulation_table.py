import pytz
from datetime import timedelta, datetime
import dateutil.parser

import logging
import numpy as np
import pandas as pd
from src.utils.sql_util import (
    get_db_conn,
    ModifyDBTable,
    get_center_info,
    get_caller_info,
    get_number_nspl_in_state,
    get_abandonment_probability_by_minutes,
    center_historical_disposition_estimate,
)
from config.project_constants import ROUTING_LEVEL_SCHEMA_NAME, ROLE_NAME


class PopulateSimulationTable(ModifyDBTable):
    def __init__(self, db_conn, schema_name, table_name):
        """Populates the simulated data in the database.
        * Inherits the `ModifyDBTable` class from `src/utils/sql_util.py`.
        * Ensures that data is inserted for unique rows only.
        * Reduces the sparseness of the data to be updated by concatenating
        the pre-defined attributes that does not change during simulation but are
        needed for feature computation.
        * Get information needed for computing the features of the simulated calls.

        Keyword arguments:
            db_conn (object) -- database connection.
            schema_name (str) -- name of schema where table is located.
            table_name (str) -- name of table to modify.
        """
        super(PopulateSimulationTable, self).__init__(
            db_conn, schema_name, table_name, ROLE_NAME
        )

        # These are attributes that are not being updated by the simulator, but none-the-less
        # needed for feature calculation in the call-level pipeline. By setting them
        # to zero, it simplifies the dictionary that is sent by the simulatator.
        self.INITIALIZED_ATTRIBUTES = {
            "network_is_ll_spanish": 0,
            "network_is_va": 0,
            "network_is_ddh": 0,
            "network_is_ddh_spanish": 0,
        }

        # Estimated call abandonment probability for waiting 19+ minutes.
        self.PROB_ABANDON_BY_MIN = get_abandonment_probability_by_minutes(
            db_conn=db_conn
        )

        # Estimated call center statistics about what happened to past calls.
        self.CENTER_HISTORICAL_DISPOSITION_STAT = (
            center_historical_disposition_estimate(db_conn=db_conn)
        )

    def insert_data_into_table(self, data):
        """Concatenate the zero_initialized attributes with the given data
        and insert the data_with_zero_initialized_attributes into the <schema_name>.<table_name>.

        Keyword arguments:
            data (dict) -- dictionary whose keys are the feature/column name(s)
                            and value(s) to insert as <data>.values().

                            Example of data:
                                data = {
                                    "call_key": "e4011-20220526002924403-500",
                                    "arrived_datetime_est": "2022-05-26 00:29:24.000",
                                    "center_key": "IL460000",
                                    "termination_number": 6304823616,
                                }
        """
        data_with_initialized_attributes = self.INITIALIZED_ATTRIBUTES.copy()
        data_with_initialized_attributes.update(data)
        return super().insert_data_into_table(data_with_initialized_attributes)

    def update_row_in_table(self, data, row_identifier):
        """Update given data in table where <row_identifier> is true.
        The <row_identifier> must be unique.

        Keyword arguments:
            data (Union(dict, str)) -- data to be updated. If type(<data>) is not str, the data
                                        is formatted to the form that psql is expecting.
                                        The dictionary expects the column names as
                                        data.keys() and values to be updated as data.values().
            row_identifier (Union(dict, str)) -- data that uniquely identifies a given row.
                                                If type(<row_identifier>) is not str,
                                                the data is formatted to the form that psql is expecting.
                                                The dictionary expects the column names as row_identifier.keys()
                                                and associated values as row_identifier.values().

        Raises:
            ValueError -- if the row_identifier is not unique.
        """

        result = self.select_row_from_table(row_identifier=row_identifier)
        # Check if the returned result is greater than 1.
        if len(result) > 1:
            logging.error(f"{row_identifier} is not unique!")
            raise ValueError(f"{row_identifier} is not unique!")
        else:
            super().update_rows_in_table(data=data, row_identifier=row_identifier)

    def get_probability_abandonment(self, current_wait_minute=0, add1_wait_minute=1):
        """Calculate the probability a call will abandon within the next <add1_wait_minute> minutes,
        conditional on having already waited <current_wait_minutes>.

        Keyword arguments:
            current_wait_minute (int, optional) -- current waiting time of caller (in minutes).
                                                   Defaults to 0.
            add1_wait_minute (int, optional) -- length of wait time of caller to be estimated (in minutes).
                                                Defaults to 1.

        Returns:
            total probability of abandonment (float) -- probability of abandonment of caller in the next
                                                        <add1_wait_minute> minutes.
        """
        total_proba_abandon = 0
        total_wait = 1
        for _ in range(add1_wait_minute):
            current_wait_minute += 1
            if current_wait_minute >= max(self.PROB_ABANDON_BY_MIN.keys()):
                total_proba_abandon = (
                    total_proba_abandon
                    + total_wait
                    * self.PROB_ABANDON_BY_MIN[max(self.PROB_ABANDON_BY_MIN.keys())]
                )
                break
            abandon_next_minute = self.PROB_ABANDON_BY_MIN.get(current_wait_minute)
            total_proba_abandon = total_proba_abandon + total_wait * abandon_next_minute
            total_wait = total_wait * (1 - abandon_next_minute)
        return total_proba_abandon

    def get_routing_attempt_attributes(self, routing_attempt_id):
        """Get the attributes for given routing attempt_id.

        Keyword arguments:
            routing_attempt_id (dict) --  unique identifier for a routing attempt.
                                          Example:
                                          {
                                            "call_key": "e4289-20220526000055001-792",
                                            "caller_npanxx": 270454,
                                            "arrived_datetime_est": "2022-05-26 00:00:55.000",
                                            "center_key": "KY270000",
                                            "termination_number": "2706895324",
                                        }
        Returns:
            routing_attempt_attributes_dict (dict): dictionary with attributes of the routing attempt.
        """
        validation_keys = np.array(
            ["center_key", "call_key", "caller_npanxx", "arrived_datetime_est"]
        )
        # Check if the keys are valid. Raise assertion if not.
        key_is_valid = np.array([i in routing_attempt_id for i in validation_keys])
        assert all(
            key_is_valid
        ), f"{list(validation_keys[~key_is_valid])} not found in {routing_attempt_id}."

        # Get the information dictionary for the caller.
        caller_info_dict = get_caller_info(
            db_conn=self.db_conn, call_key=routing_attempt_id["call_key"]
        )

        # If there is no center_key, the call is sent to the National Backup network.
        if routing_attempt_id["center_key"] is None:
            routing_attempt_attributes_dict = {
                "network_is_ll": 0,
                "network_is_ll_backup": 1,
            }
        else:
            # Get information about the center.
            center_info_dict = get_center_info(
                db_conn=self.db_conn,
                center_key=routing_attempt_id["center_key"],
                termination_number=routing_attempt_id["termination_number"],
            )

            # Compute the time when the call arrived in the center using its local timezone.
            arrived_datetime_local = self.get_datetime_local(
                datetime_to_convert=routing_attempt_id["arrived_datetime_est"],
                timezone_to=center_info_dict["center_time_zone"],
            )

            # Get the part of day that the call arrived at the local call center.
            arrived_part_of_day = self.get_part_of_day(
                datetime_to_extract=arrived_datetime_local
            )

            # Get the number of nspl centers in the state where the call center is located.
            num_nspl_centers_in_center_state = get_number_nspl_in_state(
                db_conn=self.db_conn,
                state_abbrev=center_info_dict["center_state_abbrev"],
            )

            # Update the center info dictionary with the local arrived datetime, the arrived part of date
            # and the number of nspl centers in the state where the center is located.
            center_info_dict.update(
                {
                    "arrived_datetime_local": arrived_datetime_local,
                    "arrived_part_of_day": arrived_part_of_day,
                    "num_nspl_centers_in_center_state": num_nspl_centers_in_center_state,
                }
            )

            routing_attempt_attributes_dict = {
                "network_is_ll": 1,
                "network_is_ll_backup": 0,
            }

            # Update the routing attempts attributes dictionary with the center infomation dictionary.
            routing_attempt_attributes_dict.update(center_info_dict)

        # Update the routing attempts attributes dictionary with the caller infomation dictionary.
        routing_attempt_attributes_dict.update(caller_info_dict)

        return routing_attempt_attributes_dict

    def get_initiated_datetime(
        self, completed_datetime, total_ring_time_sec, attempt_number
    ):
        """Compute the initiated datetime given the <completed_datetime> and <total_ring_time_sec>.

        Keyword arguments:
            completed_datetime (str) -- time the call was completed.
            total_ring_time_sec (int) -- how long the call rung for before it was picked up
                                         or abandoned by the caller.
            attempt_number (int) -- number of times this caller has been routed.

        Returns:
            (datetime.datetime) -- time the call was initiated.
        """
        if attempt_number == 1:
            # If this is the first attempt, then the intiated datetime should
            # be the same as the completed datetime.
            initiated_datetime = completed_datetime
        else:
            initiated_datetime = dateutil.parser.parse(
                (completed_datetime - timedelta(seconds=total_ring_time_sec)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

        return initiated_datetime

    def get_datetime_local(
        self,
        datetime_to_convert,
        timezone_to,
        timezone_from="US/Eastern",
        fmt="%Y-%m-%d %H:%M:%S",
    ):
        """Convert a datetime to local time.

        Args:
            datetime_to_convert (str) -- datetime to be converted.
            timezone_to (str) -- timezone that the datetime_to_convert should be converted to.
            timezone_from (str, optional): timezone that the datetime_to_convert is currently represented as.
                                           Defaults to "US/Eastern".
            fmt (str, optional) -- how the datetime_to_convert is represented.
                                    Defaults to "%Y-%m-%d %H:%M:%S".

        Returns:
            local datetime (str) -- local datetime based on chosen timezone_to.
        """
        foreign_timezone = pytz.timezone(timezone_from)
        local_timezone = pytz.timezone(timezone_to)

        datetime_to_convert = datetime.strptime(datetime_to_convert, fmt)
        datetime_to_convert = foreign_timezone.localize(datetime_to_convert)
        datetime_local = datetime_to_convert.astimezone(local_timezone).strftime(fmt)
        return datetime_local

    def get_part_of_day(self, datetime_to_extract, fmt="%Y-%m-%d %H:%M:%S"):
        """Extract the part of day for a given datetime.

        Args:
            datetime_to_extract (str) -- datetime to be extract part of day from.
            fmt (str, optional) -- how the datetime_to_extract is represented.
                                    Defaults to "%Y-%m-%d %H:%M:%S".

        Returns:
            part_of_day (str) -- part of day that the datetime_to_extract falls into based on
                                 hardcoded thresholds i.e
                                 1. 6 < datetime_to_extract <= 12 --> morning
                                 2. 12 < datetime_to_extract <= 18 --> afternoon
                                 3. 18 < datetime_to_extract <= 24 --> evening
                                 4. datetime_to_extract > 24 --> night
        """
        datetime_to_extract_hour = str(datetime.strptime(datetime_to_extract, fmt).hour)

        if datetime_to_extract_hour > "6" and datetime_to_extract_hour <= "12":
            return "morning"
        elif datetime_to_extract_hour > "12" and datetime_to_extract_hour <= "18":
            return "afternoon"
        elif datetime_to_extract_hour > "18" and datetime_to_extract_hour <= "24":
            return "evening"
        else:
            return "night"

    def get_center_historical_disposition_estimate(
        self, center_key, termination_number, stats_of_interest
    ):
        """Get an estimated average of what happened to past
        calls in a center. If the center-termination pair is new,
        use the average across all centers.

        Args:
            center_key (str) -- alphanumeric value that identifies a center
            termination_number (int) -- value that uniquely identifies a center
            stats_of_interest (Union(str, list)) -- statistics of interest e.g answered_avg_time_to_leave.

        Returns:
            (int) -- estimated average computed by the statistics of interest for a center key
                    and termination number pair if center key exists else returns the overall average.
        """
        try:
            stats = self.CENTER_HISTORICAL_DISPOSITION_STAT.loc[
                (center_key, termination_number), stats_of_interest
            ]
        except KeyError:
            stats = self.CENTER_HISTORICAL_DISPOSITION_STAT.loc[
                :, stats_of_interest
            ].mean()
        return stats.round().astype(int)


def main():
    """Main function to exemplify how to use the class."""

    example_data_to_update_dict = {
        "caller_npanxx": 423358,
        "caller_state_abbrev": "IL",
        "center_state_abbrev": "IL",
        "initiated_datetime_est": "2022-05-26 00:29:24.000",
        "attempt_number": 1,
        "max_attempt_num": 1,
        "completed_at_center": 1,
        "answered_at_center": 1,
        "answered_in_state": 1,
        "flowout_from_center": 0,
        "time_to_leave_center": 20,
        "talk_time_center": 7,
        "time_to_answer_center": 13,
        "arrived_part_of_day": "morning",
    }

    example_row_identifier_dict = {
        "call_key": "e4011-20220526002924403-500",
        "arrived_datetime_est": "2022-05-26 00:29:24.000",
        "center_key": "IL460000",
        "termination_number": 6304823616,
    }

    db_conn = get_db_conn()

    # Initialize class.
    populate_simulation_table = PopulateSimulationTable(
        db_conn=db_conn,
        schema_name=ROUTING_LEVEL_SCHEMA_NAME,
        table_name="simulated_routing_attempts",
    )

    # Show all the columns when printing to terminal.
    pd.set_option("display.max_columns", None)

    # Delete data from the database table given some condition.
    print(f"{'*' * 10} invoking delete statement")
    populate_simulation_table.delete_row_from_table(
        row_identifier=example_row_identifier_dict
    )
    # Select data of specified columns from the database table.
    results = populate_simulation_table.select_row_from_table(
        row_identifier=example_row_identifier_dict,
        columns_to_select="call_key, caller_npanxx",
    )
    print(results)

    # Insert data into the database table.
    print(f"{'*' * 10} invoking insert statement")
    populate_simulation_table.insert_data_into_table(data=example_row_identifier_dict)
    # Select data of specified columns from the database table.
    results = populate_simulation_table.select_row_from_table(
        row_identifier=example_row_identifier_dict,
        columns_to_select="call_key, caller_npanxx",
    )
    print(results)

    # Update data in the database table given some condition.
    print(f"{'*' * 10} invoking update statement")
    populate_simulation_table.update_row_in_table(
        data=example_data_to_update_dict, row_identifier=example_row_identifier_dict
    )
    # Select data of specified columns from the database table.
    results = populate_simulation_table.select_row_from_table(
        row_identifier=example_row_identifier_dict,
        columns_to_select="call_key, caller_npanxx",
    )
    print(results)

    # Delete data from the database table given some condition.
    print(f"{'*' * 10} invoking delete statement")
    populate_simulation_table.delete_row_from_table(
        row_identifier=example_row_identifier_dict
    )
    # Select data of specified columns from the database table.
    results = populate_simulation_table.select_row_from_table(
        row_identifier=example_row_identifier_dict,
        columns_to_select="call_key, caller_npanxx",
    )
    print(results)


# main()
