-- 1. create sink table processed_events_aggregated_hw in pgAdmin
drop table processed_events_aggregated_hw; 

CREATE TABLE IF NOT EXISTS processed_events_aggregated_hw (
    session_start TIMESTAMP(3),
    host VARCHAR,
    ip VARCHAR,
    event_count BIGINT
);

-- 2. run docker-compose to start containers taskmanager, jobmanager, postgres, pgAdmin
-- 3. submit flink job: make hw_job. on localhost 8081 flink gui can see streaming job running. after about 5 min can start to check data in processed_events_aggregated_hw


-- 4. Answer these questions
-- 4.1. What is the average number of web events of a session from a user on Tech Creator?

select cast(session_start as date) as session_date, round(avg(event_count), 4) as avg_events
from processed_events_aggregated_hw
group by cast(session_start as date)
;

-- 4.2. Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

select host, cast(session_start as date) as session_date, 
		round(avg(event_count), 4) as avg_events,
		min(event_count) as min_events,
		max(event_count) as max_events
from processed_events_aggregated_hw
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host, cast(session_start as date)
;