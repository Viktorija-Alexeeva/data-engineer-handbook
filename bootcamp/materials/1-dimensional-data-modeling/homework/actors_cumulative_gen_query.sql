
insert into actors 
with last_year as (
	select * from actors
	where current_year = 1979
) ,
	this_year_films as (
	select *
	from actor_films
	where year = 1980
) ,
	this_year_agg as (  -- aggregate all films of year in 1 row
	select ActorId, Actor, year, 
			array_agg(row(film, votes, rating, filmid)::films) as films_agg,
			cast(avg(rating) as decimal(10,2)) as avg_rating
	from this_year_films
	group by ActorId, Actor, year
	)
select 
		coalesce(l.ActorId, t.ActorId) as ActorId,
		coalesce(l.Actor, t.Actor) as Actor,		
		coalesce(l.films, array[]::films[]) 
			|| case when t.year is not null 
					then films_agg::films[]
					else array[]::films[] 
				end as films,			
		case 
			when t.year is not null then 
				(case 
					when t.avg_rating > 8 then 'star'
					when t.avg_rating > 7 then 'good'
					when t.avg_rating > 6 then 'average'
					else 'bad'
				end)::quality_class
			else l.quality_class
		end as quality_class, 		
		coalesce(t.year, l.current_year+1) as current_year,
		t.year is not null as is_active

from last_year l
full outer join this_year_agg t
	on l.ActorId = t.ActorId
--where t.actor = 'Brigitte Bardot'
;