-- 3. A cumulative query to generate device_activity_datelist from events
-- user_devices_cumulated populate (manually upload every date)
WITH yesterday AS (
    SELECT * 
	FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
    today AS (
          SELECT user_id::text,
		  		e.device_id::text, 
				browser_type,				
				date(cast(event_time as timestamp))  AS today_date,
				COUNT(1) AS num_events 
			FROM events e
			join devices d
				on e.device_id = d.device_id
            WHERE date(cast(event_time as timestamp))  = DATE('2023-01-31')
            AND user_id IS NOT NULL
			AND e.device_id IS NOT NULL
			AND browser_type IS NOT NULL
         GROUP BY user_id, e.device_id, browser_type, date(cast(event_time as timestamp)) 
    )
INSERT INTO user_devices_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
	   COALESCE(t.device_id, y.device_id),
	   COALESCE(t.browser_type, y.browser_type),
       COALESCE(y.dates_active,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.user_id IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS dates_active,
       COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t 
	ON t.user_id = y.user_id
	and t.device_id = y.device_id
	and t.browser_type = y.browser_type
;