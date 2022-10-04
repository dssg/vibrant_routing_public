-- Set role to "vibrant-routing-role" so everybody has the correct permission
set role "vibrant-routing-role";

-- Create schema ":schema_name"
create schema if not exists :schema_name;

-- Create table: evaluations
-- drop table if exists :schema_name.evaluations cascade;
create table if not exists :schema_name.evaluations ( 
    evaluation_id serial not null PRIMARY KEY,
    model_path text not null,
    trial_number bigint not null,
    creation_datetime_utc timestamp not null,
    routing_table_path text not null,
    config_routing_hash text not null,
    config_feature_hash text not null,
    random_seed bigint not null,
    active_calls_start_datetime_est timestamp not null,
    active_calls_end_datetime_est timestamp not null,
    active_calls_keys text[] not null,
    active_calls_count bigint not null,
    evaluation_config_hash text,
    log_path text not null
);
create index on :schema_name.evaluations (evaluation_id);

-- Create table: metrics_network
-- drop table if exists :schema_name.metrics_network cascade;
create table if not exists :schema_name.metrics_network ( 
    evaluation_id bigint not null,
    instance text not null,
    calls_flowout_network_count bigint,
    calls_flowout_network_fraction numeric,
    answer_rate numeric,
    abandonment_rate numeric,
    flowout_rate numeric,
    answered_in_state_rate numeric,
    answered_out_state_rate numeric,
    time_to_abandon_center_avg numeric,
    time_to_abandon_center_std numeric,
    time_to_abandon_center_var numeric,
    time_to_abandon_center_median numeric,
    time_to_abandon_center_min numeric,
    time_to_abandon_center_max numeric,
    time_to_leave_center_avg numeric,
    time_to_leave_center_std numeric,
    time_to_leave_center_var numeric,
    time_to_leave_center_median numeric,
    time_to_leave_center_min numeric,
    time_to_leave_center_max numeric,
    talk_time_center_avg numeric,
    talk_time_center_std numeric,
    talk_time_center_var numeric,
    talk_time_center_median numeric,
    talk_time_center_min numeric,
    talk_time_center_max numeric,
    ring_time_center_avg numeric,
    ring_time_center_std numeric,
    ring_time_center_var numeric,
    ring_time_center_median numeric,
    ring_time_center_min numeric,
    ring_time_center_max numeric,
    time_to_answer_center_avg numeric,
    time_to_answer_center_std numeric,
    time_to_answer_center_var numeric,
    time_to_answer_center_median numeric,
    time_to_answer_center_min numeric,
    time_to_answer_center_max numeric,
    constraint evaluation_id foreign key(evaluation_id)
		references :schema_name.evaluations(evaluation_id)
);
create index on :schema_name.metrics_network (evaluation_id);

-- Create table: metrics_call_centers
-- drop table if exists :schema_name.metrics_call_centers cascade;
create table if not exists :schema_name.metrics_call_centers ( 
    evaluation_id bigint not null,
    instance text not null,
    center_key text not null,
    termination_number bigint not null,
    calls_flowout_network_count bigint,
    calls_flowout_network_fraction numeric,
    answer_rate numeric,
    abandonment_rate numeric,
    flowout_rate numeric,
    answered_in_state_rate numeric,
    answered_out_state_rate numeric,
    time_to_abandon_center_avg numeric,
    time_to_abandon_center_std numeric,
    time_to_abandon_center_var numeric,
    time_to_abandon_center_median numeric,
    time_to_abandon_center_min numeric,
    time_to_abandon_center_max numeric,
    time_to_leave_center_avg numeric,
    time_to_leave_center_std numeric,
    time_to_leave_center_var numeric,
    time_to_leave_center_median numeric,
    time_to_leave_center_min numeric,
    time_to_leave_center_max numeric,
    talk_time_center_avg numeric,
    talk_time_center_std numeric,
    talk_time_center_var numeric,
    talk_time_center_median numeric,
    talk_time_center_min numeric,
    talk_time_center_max numeric,
    ring_time_center_avg numeric,
    ring_time_center_std numeric,
    ring_time_center_var numeric,
    ring_time_center_median numeric,
    ring_time_center_min numeric,
    ring_time_center_max numeric,
    time_to_answer_center_avg numeric,
    time_to_answer_center_std numeric,
    time_to_answer_center_var numeric,
    time_to_answer_center_median numeric,
    time_to_answer_center_min numeric,
    time_to_answer_center_max numeric,
    constraint evaluation_id foreign key(evaluation_id)
		references :schema_name.evaluations(evaluation_id)
);
create index on :schema_name.metrics_call_centers (evaluation_id);
create index on :schema_name.metrics_call_centers (center_key);
create index on :schema_name.metrics_call_centers (termination_number);