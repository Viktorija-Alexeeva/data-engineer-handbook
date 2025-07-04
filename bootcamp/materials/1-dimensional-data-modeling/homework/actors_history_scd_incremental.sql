
CREATE TYPE actors_scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_year INTEGER,
                    end_year INTEGER
                        )

-- incremental query for 1979
insert into actors_history_scd
WITH last_year_scd AS (
	    SELECT * 
		FROM actors_history_scd
	    WHERE current_year = 1978
	    AND end_year = 1978
),
     historical_scd AS (
        SELECT actorid,
               quality_class,
               is_active,
               start_year,
               end_year
        FROM actors_history_scd
        WHERE current_year = 1978
        AND end_year < 1978
),
     this_year_data AS (
         SELECT * FROM actors
         WHERE current_year = 1979
),
     unchanged_records AS (
         SELECT
                ts.actorid,
                ts.quality_class,
                ts.is_active,
                ls.start_year,
                ts.current_year as end_year
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
),
     changed_records AS (
        SELECT
                ts.actorid,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_year,
                        ls.end_year
                        )::actors_scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::actors_scd_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
),
     unnested_changed_records AS (
         SELECT actorid,
                (records::actors_scd_type).quality_class,
                (records::actors_scd_type).is_active,
                (records::actors_scd_type).start_year,
                (records::actors_scd_type).end_year
         FROM changed_records
),
     new_records AS (
         SELECT
            ts.actorid,
                ts.quality_class,
                ts.is_active,
                ts.current_year AS start_year,
                ts.current_year AS end_year
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.actorid = ls.actorid
         WHERE ls.actorid IS NULL
)

SELECT *, 1979 AS current_year 
FROM (
	  SELECT *
	  FROM historical_scd

	  UNION ALL

	  SELECT *
	  FROM unchanged_records

	  UNION ALL

	  SELECT *
	  FROM unnested_changed_records

	  UNION ALL

	  SELECT *
	  FROM new_records
  ) a
--where actorid = 'nm0000001'
on conflict (actorid, start_year)
do update set
end_year = excluded.end_year,
current_year = excluded.current_year;
