-- lab 1
WITH last_season AS (
	SELECT * FROM players
	WHERE current_season = 1995
), this_season AS (
 	SELECT * FROM player_seasons
	WHERE season = 1996
)
INSERT INTO players
SELECT
	COALESCE(ls.player_name, ts.player_name) as player_name,
	COALESCE(ls.height, ts.height) as height,
	COALESCE(ls.college, ts.college) as college,
	COALESCE(ls.country, ts.country) as country,
	COALESCE(ls.draft_year, ts.draft_year) as draft_year,
	COALESCE(ls.draft_round, ts.draft_round) as draft_round,
	COALESCE(ls.draft_number, ts.draft_number) as draft_number,
	COALESCE(ls.season_stats,
		ARRAY[]::season_stats[]
		) || CASE WHEN ts.season IS NOT NULL THEN
			ARRAY[ROW(
			ts.season,
			ts.pts,
			ts.ast,
			ts.reb, ts.weight)::season_stats]
			ELSE ARRAY[]::season_stats[] END
		as season_stats,
	 CASE
		 WHEN ts.season IS NOT NULL THEN
			 (CASE WHEN ts.pts > 20 THEN 'star'
				WHEN ts.pts > 15 THEN 'good'
				WHEN ts.pts > 10 THEN 'average'
				ELSE 'bad' END)::scoring_class
		 ELSE ls.scoring_class
	 END as scoring_class,
	 case when ts.season is not null then 0
		else ls.years_since_last_season + 1
		end as years_since_last_season,
	 COALESCE(ts.season, ls.current_season+1) AS current_season,
	 ts.season IS NOT NULL as is_active
FROM last_season ls
FULL OUTER JOIN this_season ts
	ON ls.player_name = ts.player_name;


-- lab 2
WITH years AS (
	SELECT *
	FROM generate_series(1996, 2022) AS season
),
    p AS (
        SELECT
            player_name, min(season) AS first_season
        FROM player_seasons
        GROUP BY player_name
),		
    players_and_seasons AS (
        SELECT *
        FROM p
        JOIN years y 
            ON p.first_season <= y.season
),
    windowed AS (
        SELECT
            ps.player_name,
            ps.season,
            array_remove(       -- array_remove(array, value_to_remove)
                array_agg(
                    CASE
                        WHEN p1.season IS NOT NULL 
                            THEN ROW (p1.season, p1.pts, p1.ast, p1.reb, p1.weight)::season_stats
                    END
                    ) OVER (PARTITION BY ps.player_name ORDER BY COALESCE(ps.season, p1.season) ),
                    NULL
                ) AS seasons
        FROM players_and_seasons ps
        LEFT JOIN player_seasons p1
            ON ps.player_name = p1.player_name
                AND ps.season = p1.season
        ORDER BY ps.player_name, ps.season
),
    static AS (
        SELECT      player_name,
                    max(height) AS height,
                    max(college) AS college,
                    max(country) AS country,
                    max(draft_year) AS draft_year,
                    max(draft_round) AS draft_round,
                    max(draft_number) AS draft_number
        FROM player_seasons
        GROUP BY player_name
)
INSERT INTO players
SELECT
	w.player_name,
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_round,
	s.draft_number,
	seasons AS season_stats,
	CASE
		WHEN (seasons[CARDINALITY(seasons)]).pts > 20 THEN 'star'
		WHEN (seasons[CARDINALITY(seasons)]).pts > 15 THEN 'good'
		WHEN (seasons[CARDINALITY(seasons)]).pts > 10 THEN 'average'
		ELSE 'bad'
	END::scoring_class AS scoring_class,
	w.season - (seasons[CARDINALITY(seasons)]).season AS years_since_last_season,
	w.season AS current_season,
	(seasons[CARDINALITY(seasons)]).season = w.season AS is_active
FROM windowed w
JOIN STATIC s ON w.player_name = s.player_name
