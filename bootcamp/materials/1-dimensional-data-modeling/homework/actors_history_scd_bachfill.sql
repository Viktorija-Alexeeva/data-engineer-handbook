--code for backfill
insert into actors_history_scd
with with_previous as ( -- streak = how long(many seasons) player was in the current dimension
	select actorid, current_year, quality_class, is_active,
		lag(quality_class, 1) over(partition by actorid order by current_year) as previous_quality_class, 
		lag(is_active, 1) over(partition by actorid order by current_year) as previous_is_active
	from actors
	where current_year <= 1978
), 
	with_indicators as (
	select * ,
		CASE 
			WHEN quality_class <> previous_quality_class THEN 1 
		 	WHEN is_active <> previous_is_active THEN 1 
			ELSE 0 
		END as change_indicator
	from with_previous
),
	with_streaks as (
	select * ,
			sum(change_indicator) over(partition by actorid order by current_year) as streak_identifier
	from with_indicators
	)

select actorid, quality_class, is_active,
		min(current_year) as start_year,
		max(current_year) as end_year,
		1978 as current_year
from with_streaks
--where actorid = 'nm0000001'
group by actorid, streak_identifier, is_active, quality_class
order by actorid
;