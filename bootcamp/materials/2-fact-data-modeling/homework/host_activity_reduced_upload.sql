-- 8. An incremental query that loads host_activity_reduced
-- Insert into the host_activity_reduced table
INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    -- Aggregate daily site hits and user_id per host
    SELECT 
        host,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits,
		count(distinct user_id) as num_unique_visitors
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-05')
    AND host IS NOT NULL
    GROUP BY host, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM host_activity_reduced 
    WHERE month_start = DATE('2023-01-01')
)
SELECT 
    -- Select host from either daily_aggregate or yesterday_array
    COALESCE( da.host, ya.host) AS host,
    -- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Update hit_array based on existing data and new daily aggregates
    CASE 
        WHEN ya.hit_array IS NOT NULL THEN 
            ya.hit_array || ARRAY[COALESCE(da.num_site_hits,0)] 
        WHEN ya.hit_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) -- fill in array with 0 (as many as date - month_start)
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS hit_array,
	CASE 
        WHEN ya.unique_visitors_array IS NOT NULL THEN 
            ya.unique_visitors_array || ARRAY[COALESCE(da.num_unique_visitors,0)] 
        WHEN ya.unique_visitors_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) -- fill in array with 0 (as many as date - month_start)
                || ARRAY[COALESCE(da.num_unique_visitors,0)]
    END AS unique_visitors_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.host = ya.host
ON CONFLICT (host, month_start)
DO UPDATE 
	SET hit_array = EXCLUDED.hit_array, 
		unique_visitors_array = EXCLUDED.unique_visitors_array;
