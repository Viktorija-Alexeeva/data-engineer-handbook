-- 5. A DDL for hosts_cumulated table
-- create table hosts_cumulated
-- drop table hosts_cumulated ;
 CREATE TABLE hosts_cumulated (
     host text,	
     dates_active DATE[],	-- the list of dates in the past where the user was active
     date DATE,				-- the curret date for the user
     PRIMARY KEY (host, date)
 );