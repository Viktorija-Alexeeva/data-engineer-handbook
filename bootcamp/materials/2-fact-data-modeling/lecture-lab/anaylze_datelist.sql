-- analyze datelist
SELECT
       user_id,
       datelist_int,
       BIT_COUNT(datelist_int) > 0 AS monthly_active,
       BIT_COUNT(datelist_int) AS l32,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32))) > 0 AS weekly_active,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32)))  AS l7,
       BIT_COUNT(datelist_int &
       CAST('00000001111111000000000000000000' AS BIT(32))) > 0 AS weekly_active_previous_week,
	   BIT_COUNT(datelist_int &
       CAST('10000000000000000000000000000000' AS BIT(32))) > 0 AS daily_active
FROM user_datelist_int
--where user_id = '6499095000191390000';


