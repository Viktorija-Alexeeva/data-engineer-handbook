-- 2. A DDL for an user_devices_cumulated table 
-- create table user_devices_cumulated
-- drop table user_devices_cumulated ;
 CREATE TABLE user_devices_cumulated (
     user_id text,	
	 device_id text,
	 browser_type text,	 
     dates_active DATE[],	-- the list of dates in the past where the user was active
     date DATE,				-- the curret date for the user
     PRIMARY KEY (user_id, device_id, browser_type, date)
 );