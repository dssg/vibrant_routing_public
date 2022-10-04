-- Resolve Crisis EDA
-- EDA output document https://docs.google.com/document/d/1Gn3AyXW9xnj7GNXmTA4tALutjwUNnJSqsVi5pQ6FWy8/edit#

-- Get distinct center_key for center_name starting with `resolve` 
select distinct center_key
from processed.routing_attempts  
where center_name like 'resolve%'


-- Total number of data instances

select count(*)
from processed.routing_attempts  
where center_key = 'PA000412'


-- Are the calls distinct and is there more than one terminator number?
select 
	count(*) as num_calls,
	count(distinct call_key) as distinct_calls,
	count(distinct termination_number) as distinct_termination_number
from processed.routing_attempts 
where center_key = 'PA000412'


-- Minimum and Maximum arrived datetime
select 
	min(arrived_datetime_est),
	max(arrived_datetime_est)
from processed.routing_attempts  
where center_key = 'PA000412'


-- Number of call entries per year
select 
	extract(year from arrived_datetime_est) as year,
	count(call_key) as num_calls 
from processed.routing_attempts  
where center_key = 'PA000412'
group by 1


-- What is the number of calls per month in 2020 and 2021?
select 
	extract(month from arrived_datetime_est) as "month", 
	count(call_key) as "num_calls"
from processed.routing_attempts
where 
	center_key = 'PA000412' 
	and ((extract(year from arrived_datetime_est) = 2020) 
	or (extract(year from arrived_datetime_est) = 2021))
group by 1
order by 1 asc;


-- What is the network distribution for this call center?
select distinct network
from processed.routing_attempts 
where center_key = 'PA000412'


-- Average number of calls per day
select avg(number_calls)
from (
    select
    	arrived_date_est,
    	count(*) as number_calls
    from processed.routing_attempts  
    where center_key = 'PA000412'
    group by 1
 ) as c;


-- Distribution of calls per hour of day
select
	extract(hour from arrived_datetime_est) as hour,
	count(*)
from processed.routing_attempts  
where center_key = 'PA000412'
group by 1;


-- Distribution of answered and not answered calls per hour of day
select
	extract(hour from arrived_datetime_est) as hour,
	answered_at_center,
	count(*)
from processed.routing_attempts  
where center_key = 'PA000412'
group by 1, 2;


-- What is the answer, abandonment, flowout rate of this call center?
select 
	sum(answered_at_center)::float / count(call_key) as answer_rate,
	sum(abandoned_at_center)::float / count(call_key) as abandonment_rate,
	sum(flowout_from_center)::float / count(call_key) as flowout_rate
from processed.routing_attempts 
where center_key = 'PA000412'


-- Distribution of max attempt number at center
select 
	max_attempt_num,
	count(max_attempt_num) as num_calls
from processed.routing_attempts 
where center_key = 'PA000412'
group by 1


-- Is this an ACD center?
select 
    count(call_key) as count_call_key,
	sum(center_is_acd) as sum_acd,
	sum(center_is_aa) as sum_aa
from processed.routing_attempts 
where center_key = 'PA000412'


-- Distribution of ACD-ness across the year?
select 
	extract(year from arrived_datetime_est) as year,
	count(call_key) as num_call_entries,
	sum(center_is_acd) as sum_acd,
	sum(center_is_aa) as sum_aa
from processed.routing_attempts  
where center_key = 'PA000412'
group by 1


-- Average answer/abandonment/flowout rate per attempt number
select 
	max_attempt_num,
	sum(answered_at_center)::float / count(call_key) as answer_rate,
	sum(abandoned_at_center)::float / count(call_key) as abandonment_rate,
	sum(flowout_from_center)::float / count(call_key) as flowout_rate
from processed.routing_attempts 
where center_key = 'PA000412'
group by 1


-- Call not answered at center, nor abandoned nor flowed out 
select 
	call_key,
	completed_at_center,
	talk_time_center,
from processed.routing_attempts ra 
where 
	center_key = 'PA000412'
	and answered_at_center = 0
	and abandoned_at_center = 0
	and flowout_from_center = 0


-- Distribution of caller’s county based on exchange code
select 
	caller_county,
	count(caller_county)
from processed.routing_attempts 
where center_key = 'PA000412'
group by 1
order by 2 desc


-- Mean talk/answer time of callers
select
	avg(talk_time_center)/60 as mean_talk_time_minutes,
	avg(time_to_answer_center) as mean_time_to_answer_secs
from processed.routing_attempts
where 
	center_key = 'PA000412' 
	and talk_time_center > 0 	

	
-- Distribution of talk / answer time of callers at center
select
	talk_time_center/60 as talk_time_minutes,
	time_to_answer_center as time_to_answer_secs
from processed.routing_attempts
where center_key = 'PA000412'