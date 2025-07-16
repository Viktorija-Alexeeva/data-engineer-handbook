
-- 3. A query that uses window functions on game_details to find out the following things:


-- What is the most games a team has won in a 90 game stretch?

with aggregated as (
		select distinct game_date_est,
				g.game_id, 
				gd.team_id,
				gd.team_city, 
				case
					when team_id = ( -- team_id winner
									case 
										when home_team_wins = 1 then team_id_home
											else team_id_away end 
					) then 1
					else 0 
				end as team_is_winner
		from game_details gd
		join games g
			on gd.game_id = g.game_id 
		order by 1
)
select *,
		sum(team_is_winner) over(
						partition by team_city 
						order by game_date_est 
						rows between 89 preceding and current row
						) as games_won_in_90_game_stretch
					
from aggregated
order by  games_won_in_90_game_stretch desc 
limit 1;


-- How many games in a row did LeBron James score over 10 points a game? 

WITH lebron_games AS (
    SELECT 
        g.game_date_est,
        gd.game_id,
        gd.pts,
        ROW_NUMBER() OVER (ORDER BY g.game_id) AS rn_all,
        SUM(CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END) OVER (ORDER BY g.game_id ROWS UNBOUNDED PRECEDING) AS rn_gt10
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
	streaks AS (
    SELECT *,
           rn_all - rn_gt10 AS grp
    FROM lebron_games
    WHERE pts > 10
)
select game_date_est, game_id, pts, 
	count(1) over(partition by grp) as count_games
from streaks
order by 4 desc
limit 1;

