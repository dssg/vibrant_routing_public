-- Set role to "vibrant-routing-role" so everybody has the correct permission
set role "vibrant-routing-role";

-- Create schema ":schema_name"
create schema if not exists :schema_name;

-- Create table: models
-- drop table if exists :schema_name.models cascade;
create table if not exists :schema_name.models ( 
    model_id serial not null PRIMARY KEY,
    model_class text not null,
    creation_datetime_est timestamp not null,
    pickle_path text not null,
    parameters jsonb not null,
    label text not null,
    features text[] not null,
    train_start_datetime_est timestamp not null,
    train_end_datetime_est timestamp not null,
    train_matrix_path text not null,
    log_path text not null
);
create index model_id on :schema_name.models (model_id);

-- Create table: evaluations
--drop table if exists :schema_name.evaluations;
create table if not exists :schema_name.evaluations (
    experiment_id serial not null PRIMARY KEY,
    model_id bigint not null,
    creation_datetime_est timestamp not null,
    evaluation_start_datetime_est timestamp not null,
    evaluation_end_datetime_est timestamp not null,
    evaluation_matrix_path text not null,
	constraint fk_model_id foreign key(model_id)
		references :schema_name.models(model_id)
);
create index experiment_id on :schema_name.evaluations (experiment_id);
create index evaluations_model_id on :schema_name.evaluations (model_id);

-- Create table: metrics
--drop table if exists :schema_name.metrics;
create table if not exists :schema_name.metrics (
    id serial not null PRIMARY KEY,
    experiment_id bigint not null,
    model_id bigint not null,
    creation_datetime_est timestamp not null,
    metric text,
    k float,
    best_value float,
    worst_value float,
    stochastic_value float,
    stochastic_std float,
	constraint fk_experiment_id foreign key(experiment_id)
		references :schema_name.evaluations(experiment_id),
    constraint fk_model_id foreign key(model_id)
		references :schema_name.models(model_id)
);

create index metrics_experiment_id on :schema_name.metrics (experiment_id);
create index metrics_model_id on :schema_name.metrics (model_id);


-- Create table: predictions
--drop table if exists :schema_name.predictions;
create table if not exists :schema_name.predictions (
    id serial not null PRIMARY KEY,
    experiment_id bigint not null,
    model_id bigint not null,
    routing_attempts_id bigint not null,
    y_true float not null,
    y_predicted float not null,
	constraint fk_experiment_id foreign key(experiment_id)
		references :schema_name.evaluations(experiment_id)
);
create index predictions_experiment_id on :schema_name.predictions (experiment_id);
create index predictions_model_id on :schema_name.predictions (model_id);

-- Create table: feature_importance
--drop table if exists :schema_name.feature_importance;
create table if not exists :schema_name.feature_importance ( 
    model_id bigint not null,
    feature_name text not null,
    feature_importance float not null,
    constraint fk_model_id foreign key(model_id)
		references :schema_name.models(model_id)
);
create index on :schema_name.feature_importance (model_id);