  SELECT player_name,
         UNNEST(seasons) -- CROSS JOIN UNNEST
         -- / LATERAL VIEW EXPLODE
  FROM players
  WHERE current_season = 1998
  AND player_name = 'Michael Jordan';


-- unnest array 
--variant 1
  SELECT player_name,
         UNNEST(season_stats) 
  FROM players
  WHERE current_season = 1996
  AND player_name = 'Michael Jordan';

--variant 2
with unnested as (
	select player_name, 
		unnest(season_stats)::season_stats as season_stats
	from players
	where current_season = 1996
	AND player_name = 'Michael Jordan'
)
select player_name, 
		(season_stats::season_stats).*
from unnested 
;