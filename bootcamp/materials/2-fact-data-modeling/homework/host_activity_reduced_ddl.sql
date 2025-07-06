-- 7. A monthly, reduced fact table DDL host_activity_reduced

-- create table host_activity_reduced
-- drop table host_activity_reduced ;
CREATE TABLE host_activity_reduced (
    host TEXT,  -- The host with activities
	month_start DATE,  -- The month of the activity (using first day of the month as a reference)
    hit_array real[],  -- Number of hits (activities) for that month.  think COUNT(1)
    unique_visitors_array real[], -- COUNT(DISTINCT user_id)
    PRIMARY KEY (host, month_start)
);