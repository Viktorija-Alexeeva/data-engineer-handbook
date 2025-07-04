WITH streak_started AS (
    SELECT player_name,
           current_season,
           scoring_class,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS did_change
    FROM players
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
                current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3
     )

     SELECT player_name, scoring_class, start_date, end_date
     FROM aggregated
     ;


--code from lab2 for backfill
insert into players_scd
with with_previous as ( -- streak = how long(many seasons) player was in the current dimension
	select player_name, current_season, scoring_class, is_active,
		lag(scoring_class, 1) over(partition by player_name order by current_season) as previous_scoring_class, 
		lag(is_active, 1) over(partition by player_name order by current_season) as previous_is_active
	from players
	where current_season <= 2021
),
	with_indicators as (
	select * ,
		CASE 
			WHEN scoring_class <> previous_scoring_class THEN 1 
		 	WHEN is_active <> previous_is_active THEN 1 
			ELSE 0 
		END as change_indicator
	from with_previous
),
	with_streaks as (
	select * ,
			sum(change_indicator) over(partition by player_name order by current_season) as streak_identifier
	from with_indicators
	)

select player_name, scoring_class, is_active,
		min(current_season) as start_season,
		max(current_season) as end_season,
		2021 as current_season
from with_streaks
--where player_name = 'Aaron Brooks'
group by player_name, streak_identifier, is_active, scoring_class
order by player_name
;