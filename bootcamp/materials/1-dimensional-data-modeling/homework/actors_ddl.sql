/*
films: An array of struct with the following fields:

film: The name of the film.
votes: The number of votes the film received.
rating: The rating of the film.
filmid: A unique identifier for each film.
*/

create type films as (
					Film TEXT,
					votes Integer,
				    Rating REAL,
				    FilmID text
) ;
/*
quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. 
It's categorized as follows:

star: Average rating > 8.
good: Average rating > 7 and ≤ 8.
average: Average rating > 6 and ≤ 7.
bad: Average rating ≤ 6.
*/

create type quality_class as ENUM ('bad', 'average', 'good', 'star') ;
								
-- create actors table
-- drop table actors ;
create table actors (		
    	ActorId Text,
		Actor TEXT,
		films films[],
		quality_class quality_class,
		current_year INTEGER,
		is_active BOOLEAN,
		PRIMARY KEY (ActorId, current_year)		
) ;