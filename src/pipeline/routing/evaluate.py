import logging
from src.utils.sql_util import get_db_conn

from config.project_constants import (
    EXPERIMENT_SCHEMA_NAME_ROUTING,
    ROUTING_LEVEL_SCHEMA_NAME,
    SOURCE_DATA_SCHEMA_NAME,
    SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME,
)


def evaluate_routing(db_conn, evaluation_id):
    """Evaluate the routing simulator for a given evaluation_id.

    Keyword arguments:
        db_conn (object) -- database connection.
        evaluation_id (int) -- identifier that characterises the configuration of the evaluation.
    """
    # Calculate values for the complete network and per call center:
    for table in ["metrics_network", "metrics_call_centers"]:
        # Set extra variables and group filling if necessary.
        if table == "metrics_network":
            extra_variables = ""
            group_filling = ""
        elif table == "metrics_call_centers":
            extra_variables = (
                "center_key as center_key, termination_number as termination_number,"
            )
            group_filling = "group by center_key, termination_number"
        else:
            logging.error(f"Unknown table:{table}.")

        # Loop to compute values for the 2 instances of interest: simulation and real.
        for instance_type in ["simulation", "real"]:
            # Set from filling based on the instance type.
            if instance_type == "simulation":
                from_filling = f"from {ROUTING_LEVEL_SCHEMA_NAME}.simulated_routing_attempts where center_key <> 'National Backup'"
            elif instance_type == "real":
                from_filling = (
                    f"from {ROUTING_LEVEL_SCHEMA_NAME}.active_calls_in_queue aciq "
                    f"left join {SOURCE_DATA_SCHEMA_NAME}.{SOURCE_DATA_ROUTING_ATTEMTPS_TABLE_NAME} "
                    f"   using(call_key, caller_npanxx, initiated_datetime_est)"
                )
            else:
                logging.error(f"Unknown instance type:{instance_type}.")

            # Set query.
            query = f"""
                insert into {EXPERIMENT_SCHEMA_NAME_ROUTING}.{table}
                select 
                    {evaluation_id} as evaluation_id,
                    '{instance_type}' as instance,
                    {extra_variables}
                    count(distinct call_key) - sum(completed_at_center) as calls_flowout_network_count,
                    (count(distinct call_key) - sum(completed_at_center))::float / count(distinct call_key) as calls_flowout_network_fraction,
                    sum(answered_at_center)::float / count(distinct call_key) as answer_rate,
                    sum(abandoned_at_center)::float / count(distinct call_key) as abandonment_rate,
                    sum(flowout_from_center)::float / count(distinct call_key) as flowout_rate,
                    sum(answered_in_state)::float / count(distinct call_key) as answered_in_state_rate,
                    sum(answered_out_state)::float / count(distinct call_key) as answered_out_state_rate,
                    avg(time_to_abandon_center) as time_to_abandon_center_avg,
                    stddev_samp(time_to_abandon_center) as time_to_abandon_center_std,
                    var_samp(time_to_abandon_center) as time_to_abandon_center_var,
                    percentile_cont(0.5) within group(order by time_to_abandon_center) as time_to_abandon_center_median,
                    min(time_to_abandon_center) as time_to_abandon_center_min,
                    max(time_to_abandon_center) as time_to_abandon_center_max,
                    avg(time_to_leave_center) as time_to_leave_center_avg,
                    stddev_samp(time_to_leave_center) as time_to_leave_center_std,
                    var_samp(time_to_leave_center) as time_to_leave_center_var,
                    percentile_cont(0.5) within group(order by time_to_leave_center) as time_to_leave_center_median,
                    min(time_to_leave_center) as time_to_leave_center_min,
                    max(time_to_leave_center) as time_to_leave_center_max,
                    avg(talk_time_center) as talk_time_center_avg,
                    stddev_samp(talk_time_center) as talk_time_center_std,
                    var_samp(talk_time_center) as talk_time_center_var,
                    percentile_cont(0.5) within group(order by talk_time_center) as talk_time_center_median,
                    min(talk_time_center) as talk_time_center_min,
                    max(talk_time_center) as talk_time_center_max,
                    avg(ring_time_center) as ring_time_center_avg,
                    stddev_samp(ring_time_center) as ring_time_center_std,
                    var_samp(ring_time_center) as ring_time_center_var,
                    percentile_cont(0.5) within group(order by ring_time_center) as ring_time_center_median,
                    min(ring_time_center) as ring_time_center_min,
                    max(ring_time_center) as ring_time_center_max,
                    avg(time_to_answer_center) as time_to_answer_center_avg,
                    stddev_samp(time_to_answer_center) as time_to_answer_center_std,
                    var_samp(time_to_answer_center) as time_to_answer_center_var,
                    percentile_cont(0.5) within group(order by time_to_answer_center) as time_to_answer_center_median,
                    min(time_to_answer_center) as time_to_answer_center_min,
                    max(time_to_answer_center) as time_to_answer_center_max
                {from_filling}
                {group_filling};
                """
            try:
                # print(query)
                db_conn.execute(query)
                logging.debug(f"Entry succesfully added.")
            except:
                logging.error(f"Entry failed to be added.")


# evaluate_routing(db_conn=get_db_conn(), evaluation_id=1)
