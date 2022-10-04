-- Set role to "vibrant-routing-role" so everybody has the correct permission
set role "vibrant-routing-role";

-- Create schema "processed"
create schema if not exists processed;

/* Create table: census_data_state
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'census_data_state' AND table_schema = 'raw';
 */
create table processed.census_data_state as (
	select 
		state::text,
		bsd.state_abbrev::text as state_abbrev,
		state_id::text,
		population_size::bigint,
		median_hh_income::bigint,
		gini_index::double precision,		
		race_white::float / nullif(population_size::float, 0) * 100 as race_white_perc,
		race_black_or_african_american::float / nullif(population_size::float, 0) * 100 as race_black_or_african_american_perc,
		race_american_indian_and_alaska_native::float / nullif(population_size::float, 0) * 100 as race_american_indian_and_alaska_native_perc,
		race_asian::float / nullif(population_size::float, 0) * 100 as race_asian_perc,
		race_native_hawaiian_and_other_pacific_islander::float / nullif(population_size::float, 0) * 100 as race_native_hawaiian_and_other_pacific_islander_perc,
		race_hisp_latino::float / nullif(population_size::float, 0) * 100 as race_hisp_latino_perc,
		race_some_other::float / nullif(population_size::float, 0) * 100 as race_some_other_perc,
		race_two_races_or_more::float / nullif(population_size::float, 0) * 100 as race_two_races_or_more_perc,
		born_us::float / nullif(population_size::float, 0) * 100 as born_us_perc,
		born_non_us::float / nullif(population_size::float, 0) * 100 as born_non_us_perc,
		total_male::float / nullif(population_size::float, 0) * 100 as total_male_perc,
		total_male_under_5_years::float / nullif(population_size::float, 0) * 100 as total_male_under_5_years_perc,
		total_male_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_male_5_to_9_years_perc,
		total_male_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_male_10_to_14_years_perc,
		total_male_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_male_15_to_17_years_perc,
		total_male_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_male_18_and_19_years_perc,
		total_male_20_years::float / nullif(population_size::float, 0) * 100 as total_male_20_years_perc,
		total_male_21_years::float / nullif(population_size::float, 0) * 100 as total_male_21_years_perc,
		total_male_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_male_22_to_24_years_perc,
		total_male_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_male_25_to_29_years_perc,
		total_male_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_male_30_to_34_years_perc,
		total_male_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_male_35_to_39_years_perc,
		total_male_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_male_40_to_44_years_perc,
		total_male_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_male_45_to_49_years_perc,
		total_male_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_male_50_to_54_years_perc,
		total_male_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_male_55_to_59_years_perc,
		total_male_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_male_60_and_61_years_perc,
		total_male_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_male_62_to_64_years_perc,
		total_male_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_male_65_and_66_years_perc,
		total_male_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_male_67_to_69_years_perc,
		total_male_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_male_70_to_74_years_perc,
		total_male_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_male_75_to_79_years_perc,
		total_male_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_male_80_to_84_years_perc,
		total_male_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_male_85_years_and_over_perc,
		total_female::float / nullif(population_size::float, 0) * 100 as total_female_perc,
		total_female_under_5_years::float / nullif(population_size::float, 0) * 100 as total_female_under_5_years_perc,
		total_female_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_female_5_to_9_years_perc,
		total_female_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_female_10_to_14_years_perc,
		total_female_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_female_15_to_17_years_perc,
		total_female_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_female_18_and_19_years_perc,
		total_female_20_years::float / nullif(population_size::float, 0) * 100 as total_female_20_years_perc,
		total_female_21_years::float / nullif(population_size::float, 0) * 100 as total_female_21_years_perc,
		total_female_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_female_22_to_24_years_perc,
		total_female_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_female_25_to_29_years_perc,
		total_female_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_female_30_to_34_years_perc,
		total_female_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_female_35_to_39_years_perc,
		total_female_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_female_40_to_44_years_perc,
		total_female_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_female_45_to_49_years_perc,
		total_female_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_female_50_to_54_years_perc,
		total_female_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_female_55_to_59_years_perc,
		total_female_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_female_60_and_61_years_perc,
		total_female_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_female_62_to_64_years_perc,
		total_female_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_female_65_and_66_years_perc,
		total_female_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_female_67_to_69_years_perc,
		total_female_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_female_70_to_74_years_perc,
		total_female_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_female_75_to_79_years_perc,
		total_female_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_female_80_to_84_years_perc,
		total_female_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_female_85_years_and_over_perc,
		(male_under_5_years_with_disability + male_5_to_17_years_with_disability + male_18_to_34_years_with_disability + male_35_to_64_years_with_disability + male_65_to_74_years_with_disability + male_75_years_and_over_with_disability)::float / nullif(male_disability_total_sample::float, 0) * 100 as male_with_disability_perc,
		(female_under_5_years_with_disability + female_5_to_17_years_with_disability + female_18_to_34_years_with_disability + female_35_to_64_years_with_disability + female_65_to_74_years_with_disability + female_75_years_and_over_with_disability)::float / nullif(female_disability_total_sample::float, 0) * 100 as female_with_disability_perc,
		in_household::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_perc,
		in_household_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_family_perc,
		in_household_non_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_non_family_perc,
		job_in_the_labor_force_employed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_employed_perc,
		job_in_the_labor_force_unemployed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_unemployed_perc,
		job_not_in_labor_force::float / nullif(job_total_sample::float, 0) * 100 as job_not_in_labor_force_perc,
		household_income_past_12_months_adjusted_less_than_usd_10000::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_less_than_usd10000_perc,
		household_income_past_12_months_adjusted_usd_10000_to_14999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd10000_to_14999_perc,
		household_income_past_12_months_adjusted_usd_15000_to_19999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd15000_to_19999_perc,
		household_income_past_12_months_adjusted_usd_20000_to_24999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd20000_to_24999_perc,
		household_income_past_12_months_adjusted_usd_25000_to_29999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd25000_to_29999_perc,
		household_income_past_12_months_adjusted_usd_30000_to_34999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd30000_to_34999_perc,
		household_income_past_12_months_adjusted_usd_35000_to_39999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd35000_to_39999_perc,
		household_income_past_12_months_adjusted_usd_40000_to_44999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd40000_to_44999_perc,
		household_income_past_12_months_adjusted_usd_45000_to_49999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd45000_to_49999_perc,
		household_income_past_12_months_adjusted_usd_50000_to_59999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd50000_to_59999_perc,
		household_income_past_12_months_adjusted_usd_60000_to_74999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd60000_to_74999_perc,
		household_income_past_12_months_adjusted_usd_75000_to_99999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd75000_to_99999_perc,
		household_income_past_12_months_adjusted_usd_100000_to_124999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd100000_to_124999_perc,
		household_income_past_12_months_adjusted_usd_125000_to_149999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd125000_to_149999_perc,
		household_income_past_12_months_adjusted_usd_150000_to_199999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd150000_to_199999_perc,
		household_income_past_12_months_adjusted_usd_200000_or_more::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd200000_or_more_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_no_perc,
		(health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_yes_perc,
		(health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_no_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes + health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no + health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_no_perc,
		poverty_status_under_1dot38_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_under_1dot38_of_poverty_threshold_perc,
		poverty_status_1dot38_to_1dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_1dot38_to_1dot99_of_poverty_threshold_perc,
		poverty_status_2dot00_to_3dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_2dot00_to_3dot99_of_poverty_threshold_perc,
		poverty_status_4dot00_of_poverty_threshold_and_over::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_4dot00_of_poverty_threshold_and_over_perc,
		bsd.suicides_2020::bigint as suicides_2020		
	from raw.census_data_state cds
		left join raw.basic_state_data bsd on cds.state = bsd.state_full_name
);

-- Create indexes: 
create index on processed.census_data_state (state);
create index on processed.census_data_state (state_abbrev);


/* Create table: routing_attempts
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'vibrant_centers_calls_202206031630' AND table_schema = 'raw';
 */

drop table if exists processed.routing_attempts;

create table processed.routing_attempts as (
	select 
		vccim.routing_attempts_id::bigint,
		call_key::text,
		center_key::text,
		termination_number::bigint,
		network::text,
		center_is_acd::int,
		center_is_aa::int,
		answered_at_center::int,
		answered_in_state::int,
		answered_out_state::int,
		in_state_exception::int,
		abandoned_at_center::int,
		flowout_from_center::int,
		--terminated::bigint,  -- This attribute is omitted since it doesn't add value (terminated = 1 for all entries).
		completed_at_center::bigint,
		bs_at_center::bigint,
		na_at_center::bigint,
		dc_at_center::bigint,
		p1_at_center::bigint,
		time_to_answer_center::double precision,
		time_to_leave_center::double precision,
		talk_time_center::double precision,
		ring_time_center::double precision,
		time_to_abandon_center::double precision,
		attempt_number::int,
		max_attempt_num::int,
		center_name::text,
		center_state::text as center_state_abbrev,
		cd_center.state::text as center_state_full_name, 
		cd_center.population_size::bigint as center_state_population_size, 
		cd_center.suicides_2020::bigint as center_state_suicides_2020,
		center_city::text,
		center_county::text,
		center_zip::text,
		center_fips::double precision,
		center_lat::double precision,
		center_lng::double precision,
		center_time_zone::text,
		center_uses_dst::bigint,
		center_visn::double precision,
		network_is_ll::bigint,
		network_is_va::bigint,
		network_is_ll_spanish::bigint,
		network_is_ll_backup::bigint,
		network_is_ddh::bigint,
		network_is_ddh_spanish::bigint,
		dnis::double precision,
		caller_ivr_selection::double precision,
		caller_npanxx::double precision,
		caller_area_code::double precision,
		caller_is_cell_phone::bigint,
		caller_state::text as caller_state_abbrev,
		cd_caller.state::text as caller_state_full_name, 
		cd_caller.population_size::bigint as caller_state_population_size, 
		cd_caller.suicides_2020::bigint as caller_state_suicides_2020,
		caller_city::text,
		caller_county::text,
		caller_zip::double precision,
		caller_fips::double precision,
		caller_visn::double precision,
		caller_lat::double precision,
		caller_lng::double precision,
		caller_time_zone::text,
		caller_offered_datetime_local::timestamp,
		connected_datetime_utc::timestamp,
		arrived_datetime_local::timestamp,
		arrived_date_local::date,
		--arrived_month_year_local::text,  -- This attribute is omitted since it is redundant and doesn't add value.
		arrived_datetime_est::timestamp,
		arrived_date_est::date,
		--arrived_month_year_est::text, -- This attribute is omitted since it is redundant and doesn't add value.
		arrived_datetime_utc::timestamp,
		initiated_datetime_utc::timestamp,
		initiated_datetime_est::timestamp,
		initiated_date_est::date,
		--initiated_month_year_est::text -- This attribute is omitted since it is redundant and doesn't add value.
		(ring_time_center::double precision * interval '1 second' + arrived_datetime_est::timestamp) as datetime_to_disposition_est,
		(time_to_leave_center::double precision * interval '1 second' + arrived_datetime_est::timestamp) as datetime_to_leave_center_est,
		num_NSPL_centers_in_center_state::bigint,
		--begin: the following section are imputed columns
		CAST(arrived_datetime_local as time) as arrived_time_local,
		case
			when CAST(arrived_datetime_local as time) between '6:00:00.001' and '12:00:00.000' then 'morning'::text
			when CAST(arrived_datetime_local as time) between '12:00:00.001' and '18:00:00.000' then 'afternoon'::text
			when CAST(arrived_datetime_local as time) between '18:00:00.001' and '23:59:59.000' then 'evening'::text
			else 'night'::text
		end as arrived_part_of_day,
		--initiated time of day is based on EST because local timestamp is not available
		CAST(initiated_datetime_est as time) as initiated_time_est,
		case
			when CAST(initiated_datetime_est as time) between '6:00:00.001' and '12:00:00.000' then 'morning'::text
			when CAST(initiated_datetime_est as time) between '12:00:00.001' and '18:00:00.000' then 'afternoon'::text
			when CAST(initiated_datetime_est as time) between '18:00:00.001' and '23:59:59.000' then 'evening'::text
			else 'night'::text
		end as initiated_part_of_day
		--end: the above section are imputed columns
		from raw.vibrant_centers_calls_202206031630 vcc
			left join processed.census_data_state cd_center on cd_center.state_abbrev = vcc.center_state 
			left join processed.census_data_state cd_caller on cd_caller.state_abbrev = vcc.caller_state
			left join processed.state_center_data scd on scd.state_abbrev = vcc.center_state
			left join raw.vibrant_centers_calls_202206031630_id_mapping vccim using(call_key, arrived_datetime_est, center_key, termination_number)
		where terminated = 1 and vcc.network = 'NSPL' and vcc.max_attempt_num <= 6
			-- Terminated = 1 stands for calls that were terminated in the Vibrant network.
			-- Network = NSPL means that we are excluding national backups and the Spanish network.
			-- Max attempt num <= 6 means that we only consider calls that are routed to at most 6 call centers. This is done to reduce noise.
			-- This filter is leaving out 1,292,993 distinct calls (which are split into 1,478,138 routing attempts)
);

-- Create indexes: 
create index call_key on processed.routing_attempts (call_key);
create index termination_number on processed.routing_attempts (termination_number);
create index routing_attempts_id on processed.routing_attempts (routing_attempts_id);


/* Create table: routing_table
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'vibrant_routingtable_202206031725' AND table_schema = 'raw';
 */
drop table if exists processed.routing_table;

create table processed.routing_table as (
	select 
		npanxx::int,
		center1id::text,
		center1termination::bigint,
		center1role::text,
		center2id::text,
		center2termination::bigint,
		center2role::text,
		center3id::text,
		center3termination::bigint,
		center3role::text,
		--center4id::text, -- This attribute is omitted because it is empty.
		--center4termination::bigint, -- This attribute is omitted because it is empty.
		--center4role::text, -- This attribute is omitted because it is empty.
		datechanged::date,
		datechanged::timestamp as datetime_changed
		from raw.vibrant_routingtable_202206031725 vr  
);

-- Create indexes: 
create index npanxx on processed.routing_table (npanxx);

/*
 * Create table: state_center_data
 */

drop table if exists processed.state_center_data;

create table processed.state_center_data as (
	select
		center_state_abbrev::text as state_abbrev,
		count(distinct center_key) as num_NSPL_centers_in_center_state,
		population_size,
		suicides_2020
	from processed.routing_attempts ra 
		left join processed.census_data_state cds on cds.state_abbrev = ra.center_state_abbrev
	group by center_state_abbrev, population_size, suicides_2020
);

-- Create indexes: 
create index state_abbrev on processed.state_center_data (state_abbrev);


/* Create table: area_codes
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'area_codes_by_citystate' AND table_schema = 'raw';
 */
drop table if exists processed.area_codes;

create table processed.area_codes as (
	select 
		area_code::int,
		city::text,
		state::text,
		country::text,
		latitude::double precision,
		longitude::double precision
		from raw.area_codes_by_citystate vr  
);

-- Create indexes: 
create index area_code on processed.area_codes (area_code);


/* Create table: census_data_county
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'census_data_county' AND table_schema = 'raw';
 */
create table processed.census_data_county as (
	select 
		state_id::text,
		state::text,
		bsd.state_abbrev::text as state_abbrev,
		county_id::text,
		county::text,
		population_size::bigint,
		median_hh_income::bigint,
		gini_index::double precision,		
		race_white::float / nullif(population_size::float, 0) * 100 as race_white_perc,
		race_black_or_african_american::float / nullif(population_size::float, 0) * 100 as race_black_or_african_american_perc,
		race_american_indian_and_alaska_native::float / nullif(population_size::float, 0) * 100 as race_american_indian_and_alaska_native_perc,
		race_asian::float / nullif(population_size::float, 0) * 100 as race_asian_perc,
		race_native_hawaiian_and_other_pacific_islander::float / nullif(population_size::float, 0) * 100 as race_native_hawaiian_and_other_pacific_islander_perc,
		race_hisp_latino::float / nullif(population_size::float, 0) * 100 as race_hisp_latino_perc,
		race_some_other::float / nullif(population_size::float, 0) * 100 as race_some_other_perc,
		race_two_races_or_more::float / nullif(population_size::float, 0) * 100 as race_two_races_or_more_perc,
		born_us::float / nullif(population_size::float, 0) * 100 as born_us_perc,
		born_non_us::float / nullif(population_size::float, 0) * 100 as born_non_us_perc,
		total_male::float / nullif(population_size::float, 0) * 100 as total_male_perc,
		total_male_under_5_years::float / nullif(population_size::float, 0) * 100 as total_male_under_5_years_perc,
		total_male_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_male_5_to_9_years_perc,
		total_male_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_male_10_to_14_years_perc,
		total_male_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_male_15_to_17_years_perc,
		total_male_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_male_18_and_19_years_perc,
		total_male_20_years::float / nullif(population_size::float, 0) * 100 as total_male_20_years_perc,
		total_male_21_years::float / nullif(population_size::float, 0) * 100 as total_male_21_years_perc,
		total_male_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_male_22_to_24_years_perc,
		total_male_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_male_25_to_29_years_perc,
		total_male_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_male_30_to_34_years_perc,
		total_male_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_male_35_to_39_years_perc,
		total_male_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_male_40_to_44_years_perc,
		total_male_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_male_45_to_49_years_perc,
		total_male_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_male_50_to_54_years_perc,
		total_male_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_male_55_to_59_years_perc,
		total_male_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_male_60_and_61_years_perc,
		total_male_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_male_62_to_64_years_perc,
		total_male_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_male_65_and_66_years_perc,
		total_male_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_male_67_to_69_years_perc,
		total_male_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_male_70_to_74_years_perc,
		total_male_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_male_75_to_79_years_perc,
		total_male_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_male_80_to_84_years_perc,
		total_male_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_male_85_years_and_over_perc,
		total_female::float / nullif(population_size::float, 0) * 100 as total_female_perc,
		total_female_under_5_years::float / nullif(population_size::float, 0) * 100 as total_female_under_5_years_perc,
		total_female_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_female_5_to_9_years_perc,
		total_female_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_female_10_to_14_years_perc,
		total_female_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_female_15_to_17_years_perc,
		total_female_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_female_18_and_19_years_perc,
		total_female_20_years::float / nullif(population_size::float, 0) * 100 as total_female_20_years_perc,
		total_female_21_years::float / nullif(population_size::float, 0) * 100 as total_female_21_years_perc,
		total_female_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_female_22_to_24_years_perc,
		total_female_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_female_25_to_29_years_perc,
		total_female_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_female_30_to_34_years_perc,
		total_female_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_female_35_to_39_years_perc,
		total_female_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_female_40_to_44_years_perc,
		total_female_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_female_45_to_49_years_perc,
		total_female_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_female_50_to_54_years_perc,
		total_female_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_female_55_to_59_years_perc,
		total_female_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_female_60_and_61_years_perc,
		total_female_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_female_62_to_64_years_perc,
		total_female_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_female_65_and_66_years_perc,
		total_female_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_female_67_to_69_years_perc,
		total_female_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_female_70_to_74_years_perc,
		total_female_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_female_75_to_79_years_perc,
		total_female_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_female_80_to_84_years_perc,
		total_female_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_female_85_years_and_over_perc,
		(male_under_5_years_with_disability + male_5_to_17_years_with_disability + male_18_to_34_years_with_disability + male_35_to_64_years_with_disability + male_65_to_74_years_with_disability + male_75_years_and_over_with_disability)::float / nullif(male_disability_total_sample::float, 0) * 100 as male_with_disability_perc,
		(female_under_5_years_with_disability + female_5_to_17_years_with_disability + female_18_to_34_years_with_disability + female_35_to_64_years_with_disability + female_65_to_74_years_with_disability + female_75_years_and_over_with_disability)::float / nullif(female_disability_total_sample::float, 0) * 100 as female_with_disability_perc,
		in_household::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_perc,
		in_household_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_family_perc,
		in_household_non_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_non_family_perc,
		job_in_the_labor_force_employed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_employed_perc,
		job_in_the_labor_force_unemployed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_unemployed_perc,
		job_not_in_labor_force::float / nullif(job_total_sample::float, 0) * 100 as job_not_in_labor_force_perc,
		household_income_past_12_months_adjusted_less_than_usd_10000::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_less_than_usd10000_perc,
		household_income_past_12_months_adjusted_usd_10000_to_14999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd10000_to_14999_perc,
		household_income_past_12_months_adjusted_usd_15000_to_19999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd15000_to_19999_perc,
		household_income_past_12_months_adjusted_usd_20000_to_24999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd20000_to_24999_perc,
		household_income_past_12_months_adjusted_usd_25000_to_29999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd25000_to_29999_perc,
		household_income_past_12_months_adjusted_usd_30000_to_34999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd30000_to_34999_perc,
		household_income_past_12_months_adjusted_usd_35000_to_39999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd35000_to_39999_perc,
		household_income_past_12_months_adjusted_usd_40000_to_44999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd40000_to_44999_perc,
		household_income_past_12_months_adjusted_usd_45000_to_49999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd45000_to_49999_perc,
		household_income_past_12_months_adjusted_usd_50000_to_59999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd50000_to_59999_perc,
		household_income_past_12_months_adjusted_usd_60000_to_74999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd60000_to_74999_perc,
		household_income_past_12_months_adjusted_usd_75000_to_99999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd75000_to_99999_perc,
		household_income_past_12_months_adjusted_usd_100000_to_124999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd100000_to_124999_perc,
		household_income_past_12_months_adjusted_usd_125000_to_149999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd125000_to_149999_perc,
		household_income_past_12_months_adjusted_usd_150000_to_199999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd150000_to_199999_perc,
		household_income_past_12_months_adjusted_usd_200000_or_more::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd200000_or_more_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_no_perc,
		(health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_yes_perc,
		(health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_no_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes + health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no + health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_no_perc,
		poverty_status_under_1dot38_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_under_1dot38_of_poverty_threshold_perc,
		poverty_status_1dot38_to_1dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_1dot38_to_1dot99_of_poverty_threshold_perc,
		poverty_status_2dot00_to_3dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_2dot00_to_3dot99_of_poverty_threshold_perc,
		poverty_status_4dot00_of_poverty_threshold_and_over::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_4dot00_of_poverty_threshold_and_over_perc,
		bsd.suicides_2020::bigint as suicides_2020		
	from raw.census_data_county cds
		left join raw.basic_state_data bsd on cds.state = bsd.state_full_name
);

-- Create indexes: 
create index on processed.census_data_county (state_id);
create index on processed.census_data_county (state);
create index on processed.census_data_county (state_abbrev);
create index on processed.census_data_county (county_id);
create index on processed.census_data_county (county);


/* Create table: census_data_zcta
 * 
 * Query to get name of data columns:
 * SELECT column_name, data_type FROM information_schema.columns 
 * WHERE table_name = 'census_data_zcta' AND table_schema = 'raw';
 */
create table processed.census_data_zcta as (
	select 
		zcta::text,
		population_size::bigint,
		median_hh_income::bigint,
		gini_index::double precision,		
		race_white::float / nullif(population_size::float, 0) * 100 as race_white_perc,
		race_black_or_african_american::float / nullif(population_size::float, 0) * 100 as race_black_or_african_american_perc,
		race_american_indian_and_alaska_native::float / nullif(population_size::float, 0) * 100 as race_american_indian_and_alaska_native_perc,
		race_asian::float / nullif(population_size::float, 0) * 100 as race_asian_perc,
		race_native_hawaiian_and_other_pacific_islander::float / nullif(population_size::float, 0) * 100 as race_native_hawaiian_and_other_pacific_islander_perc,
		race_hisp_latino::float / nullif(population_size::float, 0) * 100 as race_hisp_latino_perc,
		race_some_other::float / nullif(population_size::float, 0) * 100 as race_some_other_perc,
		race_two_races_or_more::float / nullif(population_size::float, 0) * 100 as race_two_races_or_more_perc,
		born_us::float / nullif(population_size::float, 0) * 100 as born_us_perc,
		born_non_us::float / nullif(population_size::float, 0) * 100 as born_non_us_perc,
		total_male::float / nullif(population_size::float, 0) * 100 as total_male_perc,
		total_male_under_5_years::float / nullif(population_size::float, 0) * 100 as total_male_under_5_years_perc,
		total_male_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_male_5_to_9_years_perc,
		total_male_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_male_10_to_14_years_perc,
		total_male_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_male_15_to_17_years_perc,
		total_male_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_male_18_and_19_years_perc,
		total_male_20_years::float / nullif(population_size::float, 0) * 100 as total_male_20_years_perc,
		total_male_21_years::float / nullif(population_size::float, 0) * 100 as total_male_21_years_perc,
		total_male_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_male_22_to_24_years_perc,
		total_male_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_male_25_to_29_years_perc,
		total_male_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_male_30_to_34_years_perc,
		total_male_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_male_35_to_39_years_perc,
		total_male_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_male_40_to_44_years_perc,
		total_male_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_male_45_to_49_years_perc,
		total_male_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_male_50_to_54_years_perc,
		total_male_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_male_55_to_59_years_perc,
		total_male_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_male_60_and_61_years_perc,
		total_male_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_male_62_to_64_years_perc,
		total_male_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_male_65_and_66_years_perc,
		total_male_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_male_67_to_69_years_perc,
		total_male_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_male_70_to_74_years_perc,
		total_male_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_male_75_to_79_years_perc,
		total_male_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_male_80_to_84_years_perc,
		total_male_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_male_85_years_and_over_perc,
		total_female::float / nullif(population_size::float, 0) * 100 as total_female_perc,
		total_female_under_5_years::float / nullif(population_size::float, 0) * 100 as total_female_under_5_years_perc,
		total_female_5_to_9_years::float / nullif(population_size::float, 0) * 100 as total_female_5_to_9_years_perc,
		total_female_10_to_14_years::float / nullif(population_size::float, 0) * 100 as total_female_10_to_14_years_perc,
		total_female_15_to_17_years::float / nullif(population_size::float, 0) * 100 as total_female_15_to_17_years_perc,
		total_female_18_and_19_years::float / nullif(population_size::float, 0) * 100 as total_female_18_and_19_years_perc,
		total_female_20_years::float / nullif(population_size::float, 0) * 100 as total_female_20_years_perc,
		total_female_21_years::float / nullif(population_size::float, 0) * 100 as total_female_21_years_perc,
		total_female_22_to_24_years::float / nullif(population_size::float, 0) * 100 as total_female_22_to_24_years_perc,
		total_female_25_to_29_years::float / nullif(population_size::float, 0) * 100 as total_female_25_to_29_years_perc,
		total_female_30_to_34_years::float / nullif(population_size::float, 0) * 100 as total_female_30_to_34_years_perc,
		total_female_35_to_39_years::float / nullif(population_size::float, 0) * 100 as total_female_35_to_39_years_perc,
		total_female_40_to_44_years::float / nullif(population_size::float, 0) * 100 as total_female_40_to_44_years_perc,
		total_female_45_to_49_years::float / nullif(population_size::float, 0) * 100 as total_female_45_to_49_years_perc,
		total_female_50_to_54_years::float / nullif(population_size::float, 0) * 100 as total_female_50_to_54_years_perc,
		total_female_55_to_59_years::float / nullif(population_size::float, 0) * 100 as total_female_55_to_59_years_perc,
		total_female_60_and_61_years::float / nullif(population_size::float, 0) * 100 as total_female_60_and_61_years_perc,
		total_female_62_to_64_years::float / nullif(population_size::float, 0) * 100 as total_female_62_to_64_years_perc,
		total_female_65_and_66_years::float / nullif(population_size::float, 0) * 100 as total_female_65_and_66_years_perc,
		total_female_67_to_69_years::float / nullif(population_size::float, 0) * 100 as total_female_67_to_69_years_perc,
		total_female_70_to_74_years::float / nullif(population_size::float, 0) * 100 as total_female_70_to_74_years_perc,
		total_female_75_to_79_years::float / nullif(population_size::float, 0) * 100 as total_female_75_to_79_years_perc,
		total_female_80_to_84_years::float / nullif(population_size::float, 0) * 100 as total_female_80_to_84_years_perc,
		total_female_85_years_and_over::float / nullif(population_size::float, 0) * 100 as total_female_85_years_and_over_perc,
		(male_under_5_years_with_disability + male_5_to_17_years_with_disability + male_18_to_34_years_with_disability + male_35_to_64_years_with_disability + male_65_to_74_years_with_disability + male_75_years_and_over_with_disability)::float / nullif(male_disability_total_sample::float, 0) * 100 as male_with_disability_perc,
		(female_under_5_years_with_disability + female_5_to_17_years_with_disability + female_18_to_34_years_with_disability + female_35_to_64_years_with_disability + female_65_to_74_years_with_disability + female_75_years_and_over_with_disability)::float / nullif(female_disability_total_sample::float, 0) * 100 as female_with_disability_perc,
		in_household::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_perc,
		in_household_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_family_perc,
		in_household_non_family::float / nullif(in_household_total_sample::float, 0) * 100 as in_household_non_family_perc,
		job_in_the_labor_force_employed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_employed_perc,
		job_in_the_labor_force_unemployed::float / nullif(job_total_sample::float, 0) * 100 as job_in_the_labor_force_unemployed_perc,
		job_not_in_labor_force::float / nullif(job_total_sample::float, 0) * 100 as job_not_in_labor_force_perc,
		household_income_past_12_months_adjusted_less_than_usd_10000::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_less_than_usd10000_perc,
		household_income_past_12_months_adjusted_usd_10000_to_14999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd10000_to_14999_perc,
		household_income_past_12_months_adjusted_usd_15000_to_19999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd15000_to_19999_perc,
		household_income_past_12_months_adjusted_usd_20000_to_24999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd20000_to_24999_perc,
		household_income_past_12_months_adjusted_usd_25000_to_29999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd25000_to_29999_perc,
		household_income_past_12_months_adjusted_usd_30000_to_34999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd30000_to_34999_perc,
		household_income_past_12_months_adjusted_usd_35000_to_39999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd35000_to_39999_perc,
		household_income_past_12_months_adjusted_usd_40000_to_44999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd40000_to_44999_perc,
		household_income_past_12_months_adjusted_usd_45000_to_49999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd45000_to_49999_perc,
		household_income_past_12_months_adjusted_usd_50000_to_59999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd50000_to_59999_perc,
		household_income_past_12_months_adjusted_usd_60000_to_74999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd60000_to_74999_perc,
		household_income_past_12_months_adjusted_usd_75000_to_99999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd75000_to_99999_perc,
		household_income_past_12_months_adjusted_usd_100000_to_124999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd100000_to_124999_perc,
		household_income_past_12_months_adjusted_usd_125000_to_149999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd125000_to_149999_perc,
		household_income_past_12_months_adjusted_usd_150000_to_199999::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd150000_to_199999_perc,
		household_income_past_12_months_adjusted_usd_200000_or_more::float / nullif(household_income_past_12_months_adjusted_total_sample::float, 0) * 100 as household_income_past12months_adjusted_usd200000_or_more_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no)::float / nullif(health_insurance_total_male_sample::float, 0) * 100 as health_insurance_male_no_perc,
		(health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_yes_perc,
		(health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_female_sample::float, 0) * 100 as health_insurance_female_no_perc,
		(health_insurance_male_under_6_years_yes + health_insurance_male_6_to_17_years_yes + health_insurance_male_18_to_24_years_yes + health_insurance_male_25_to_34_years_yes + health_insurance_male_35_to_44_years_yes + health_insurance_male_45_to_54_years_yes + health_insurance_male_55_to_64_years_yes + health_insurance_male_65_to_74_years_yes + health_insurance_male_75_years_and_over_yes + health_insurance_female_under_6_years_yes + health_insurance_female_6_to_17_years_yes + health_insurance_female_18_to_24_years_yes + health_insurance_female_25_to_34_years_yes + health_insurance_female_35_to_44_years_yes + health_insurance_female_45_to_54_years_yes + health_insurance_female_55_to_64_years_yes + health_insurance_female_65_to_74_years_yes + health_insurance_female_75_years_and_over_yes)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_yes_perc,
		(health_insurance_male_under_6_years_no + health_insurance_male_6_to_17_years_no + health_insurance_male_18_to_24_years_no + health_insurance_male_25_to_34_years_no + health_insurance_male_35_to_44_years_no + health_insurance_male_45_to_54_years_no + health_insurance_male_55_to_64_years_no + health_insurance_male_65_to_74_years_no + health_insurance_male_75_years_and_over_no + health_insurance_female_under_6_years_no + health_insurance_female_6_to_17_years_no + health_insurance_female_18_to_24_years_no + health_insurance_female_25_to_34_years_no + health_insurance_female_35_to_44_years_no + health_insurance_female_45_to_54_years_no + health_insurance_female_55_to_64_years_no + health_insurance_female_65_to_74_years_no + health_insurance_female_75_years_and_over_no)::float / nullif(health_insurance_total_sample::float, 0) * 100 as health_insurance_no_perc,
		poverty_status_under_1dot38_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_under_1dot38_of_poverty_threshold_perc,
		poverty_status_1dot38_to_1dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_1dot38_to_1dot99_of_poverty_threshold_perc,
		poverty_status_2dot00_to_3dot99_of_poverty_threshold::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_2dot00_to_3dot99_of_poverty_threshold_perc,
		poverty_status_4dot00_of_poverty_threshold_and_over::float / nullif(poverty_status_total_sample::float, 0) * 100 as poverty_status_4dot00_of_poverty_threshold_and_over_perc
	from raw.census_data_zcta cds
);

-- Create indexes: 
create index on processed.census_data_zcta (zcta);
