from pyspark.sql import SparkSession

query = """

with with_previous as ( 
	select actorid, current_year, quality_class, is_active,
		lag(quality_class, 1) over(partition by actorid order by current_year) as previous_quality_class, 
		lag(is_active, 1) over(partition by actorid order by current_year) as previous_is_active
	from actors
	where current_year <= 1978
), 
	with_indicators as (
	select * ,
		CASE 
			WHEN quality_class <> previous_quality_class THEN 1 
		 	WHEN is_active <> previous_is_active THEN 1 
			ELSE 0 
		END as change_indicator
	from with_previous
),
	with_streaks as (
	select * ,
			sum(change_indicator) over(partition by actorid order by current_year) as streak_identifier
	from with_indicators
	)

select actorid, quality_class, is_active,
		min(current_year) as start_year,
		max(current_year) as end_year,
		cast(1978 as long) as current_year
from with_streaks
group by actorid, streak_identifier, is_active, quality_class

"""


def do_actors_streaks_transformation(spark, dataframe):
    # create tempView for source table from sql query
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_history_scd") \
        .getOrCreate()
    output_df = do_actors_streaks_transformation(spark, spark.table("actors"))
    # insert into actors_history_scd
    output_df.write.mode("overwrite").insertInto("actors_history_scd")
    