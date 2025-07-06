
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-03-31') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-02-28', '2023-03-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-03-31')
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-03-31') as date
         FROM starter
         GROUP BY user_id
     )

     INSERT INTO user_datelist_int
     SELECT * FROM bits
;


-- code from lab2
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.series_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.series_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS series_date) as d
    WHERE date = DATE('2023-01-31')
	--and user_id = '439578290726747300'
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,	-- starts from -1 day from 'date' and in desc order (30-01, 29-01, 28-01)
                DATE('2023-01-31') as date
         FROM starter
         GROUP BY user_id
     )

     INSERT INTO user_datelist_int
     SELECT * 
	 FROM bits ;