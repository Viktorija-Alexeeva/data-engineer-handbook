
-- 2. A query that uses GROUPING SETS to do efficient aggregations of game_details data

-- create table game_details_statistics 
drop table if exists game_details_statistics ;
create table game_details_statistics as

with aggregated as (
		select coalesce(gd.game_id, -1 ) as game_id, 
				coalesce(gd.player_name, 'unknown') as player_name, 
				coalesce(gd.team_id, -1) as team_id,
				coalesce(gd.team_city, 'unknown') as team_city, 
				coalesce(g.season, -1 ) as season,
				case
					when team_id = ( -- team_id winner
									case 
										when home_team_wins = 1 then team_id_home
											else team_id_away end 
					) then 1
					else 0 
				end as team_is_winner, 
				pts 
		from game_details gd
		join games g
			on gd.game_id = g.game_id 
		where pts is not null
		order by 1
)

select 
		case 		-- detect columns, by which data is aggregated
			when grouping(player_name) = 0
				and grouping(team_city) = 0
					then 'player_name_team_city'
			when grouping(player_name) = 0
				and grouping(season) = 0
					then 'player_name_season'			
			when grouping(player_name) = 0 then 'player_name'
			when grouping(team_city) = 0 then 'team_city'
			when grouping(season) = 0 then 'season'
		end as aggregation_level,
		coalesce(player_name, 'All players') as player_name,
		coalesce(team_city, 'All teams') as team_city,
		coalesce(season, -1) as season,
		sum(team_is_winner) AS games_won,
		sum(pts) as total_points	
from aggregated

group by grouping sets( 
	(player_name, team_city),
	(player_name, season),
	(team_city)
)
order by sum(pts) desc
;

-- Answer questions like who scored the most points playing for one team? player and team
select player_name, team_city, total_points
from game_details_statistics
where aggregation_level = 'player_name_team_city'
order by total_points desc
limit 1; 

-- Answer questions like who scored the most points in one season? player and season
select player_name, season, total_points
from game_details_statistics
where aggregation_level = 'player_name_season'
order by total_points desc
limit 1; 

-- Answer questions like which team has won the most games? team
select team_city, games_won
from game_details_statistics
where aggregation_level = 'team_city'
order by games_won desc
limit 1
;
