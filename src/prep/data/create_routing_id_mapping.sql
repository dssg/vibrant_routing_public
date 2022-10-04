-- Set role to "vibrant-routing-role" so everybody has the correct permission
set role "vibrant-routing-role";

-- Drop center_calls id mapping table if it exists.
drop table if exists raw.vibrant_centers_calls_202206031630_id_mapping;


-- Create center_calls id mapping table.
create table raw.vibrant_centers_calls_202206031630_id_mapping as (
	with call__arrived__center__termination as (
		select 
			call_key,
			arrived_datetime_est,
			center_key, 
			termination_number
		from raw.vibrant_centers_calls_202206031630 
	)
	select 
		row_number() over (order by call_key asc) as routing_attempts_id, 
		call_key,
		arrived_datetime_est,
		center_key, 
		termination_number
	from call__arrived__center__termination
	order by call_key asc
)