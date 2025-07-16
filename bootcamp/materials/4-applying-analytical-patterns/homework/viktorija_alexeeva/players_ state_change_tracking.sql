-- 1. A query that does state change tracking for players

/*
special version of state transition track:
- new (didn't exist yesterday, active today)
- retained (active yesterday, active today)
- churned (active yesterday, inactive today)
- resurrected (inactive yesterday, active today)
- stale (inactive yesterday, inactive today)

A player entering the league should be New
A player leaving the league should be Retired (churned)
A player staying in the league should be Continued Playing (retained)
A player that comes out of retirement should be Returned from Retirement (resurrected)
A player that stays out of the league should be Stayed Retired (stale)
*/

drop table if exists players_state_change_tracking ;
--create table players_state_change_tracking
 CREATE TABLE players_state_change_tracking (
     player_name TEXT,
     start_season integer,
     end_season integer,
     season_active_state TEXT,
     seasons_active integer[],
     current_season integer,
     PRIMARY KEY (player_name, current_season)
 );


insert into players_state_change_tracking
WITH last_season AS (
    SELECT * 
	FROM players_state_change_tracking
    WHERE current_season = 1996
),
     this_season AS (
         SELECT
            player_name,
            current_season,
            COUNT(1)
         FROM players
         WHERE current_season = 1997
         AND player_name IS NOT NULL
         AND is_active = True
         GROUP BY player_name, current_season
     )

 SELECT COALESCE(t.player_name, y.player_name) as player_name,
		COALESCE(y.start_season, t.current_season) AS start_season,
		COALESCE(t.current_season, y.end_season) AS end_season,		
		CASE
			WHEN y.player_name IS NULL THEN 'New'
			WHEN y.end_season = t.current_season - 1 THEN 'Continued Playing'
			WHEN y.end_season < t.current_season - 1 THEN 'Returned from Retirement'
			WHEN t.current_season IS NULL AND y.end_season = y.current_season THEN 'Retired'
				ELSE 'Stayed Retired'
		END as season_active_state,

		COALESCE(y.seasons_active,
				 ARRAY []::integer[])
			|| CASE
				   WHEN
					   t.player_name IS NOT NULL
					   THEN ARRAY [t.current_season]
				   ELSE ARRAY []::integer[]
			END AS seasons_active,
		COALESCE(t.current_season, y.current_season + 1) as current_season
 FROM this_season t
		  FULL OUTER JOIN last_season y
						  ON t.player_name = y.player_name
;


