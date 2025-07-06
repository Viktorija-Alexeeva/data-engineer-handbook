-- 6. The incremental query to generate host_activity_datelist
-- hosts_cumulated populate (manually upload every date)
WITH yesterday AS (
    SELECT * 
	FROM hosts_cumulated
    WHERE date = DATE('2023-01-09')
),
    today AS (
          SELECT host,
                 date(cast(event_time as timestamp))  AS today_date,
                 COUNT(1) AS num_events 
			FROM events
            WHERE date(cast(event_time as timestamp))  = DATE('2023-01-10')
            AND host IS NOT NULL
         GROUP BY host,  date(cast(event_time as timestamp)) 
    )
INSERT INTO hosts_cumulated
SELECT
       COALESCE(t.host, y.host),
       COALESCE(y.dates_active,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.host IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS dates_active,
       COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM yesterday y
    FULL OUTER JOIN
    today t ON t.host = y.host;
