/*
Create a DDL for an actors_history_scd table with the following features:

Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
Tracks quality_class and is_active status for each actor in the actors table.
*/

-- create actors_history_scd
-- drop table actors_history_scd;
create table actors_history_scd
(
	ActorId Text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year INTEGER,
	primary key(ActorId, start_year)
);