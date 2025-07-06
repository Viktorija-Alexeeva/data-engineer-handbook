 CREATE TABLE users_cumulated (
     user_id text,	
     dates_active DATE[],	-- the list of dates in the past where the user was active
     date DATE,				-- the curret date for the user
     PRIMARY KEY (user_id, date)
 );