-- 4. Convert the device_activity_datelist column into a datelist_int column

-- create TABLE user_device_datelist_int
-- drop table user_device_datelist_int ;
CREATE TABLE user_device_datelist_int (
    user_id text,
	device_id text,
	browser_type text,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, device_id, browser_type, date)
);

-- generate datelist_int
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.series_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.series_date) AS days_since,
           uc.user_id,
		   uc.device_id, 
		   uc.browser_type
    FROM user_devices_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS series_date) as d
    WHERE date = DATE('2023-01-31')
	--and user_id = '10060569187331700000'
	--and device_id = '10598707916011500000'
),
     bits AS (
         SELECT user_id, device_id, browser_type,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,	-- starts from -1 day from 'date' and in desc order (30-01, 29-01, 28-01)
                DATE('2023-01-31') as date
         FROM starter
         GROUP BY user_id, device_id, browser_type
     )

     INSERT INTO user_device_datelist_int
     SELECT * 
	 FROM bits ;
