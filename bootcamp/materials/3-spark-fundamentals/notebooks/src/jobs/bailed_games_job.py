from pyspark.sql import SparkSession

query = """

select dim_player_name, 
		count(1) as num_games,
		count(case when dim_not_with_team then 1 end)as bailed_num,
		cast(count(case when dim_not_with_team then 1 end) as real)/count(1) as bail_pct
from fct_game_details 
group by dim_player_name
order by dim_player_name
"""


def do_bailed_games_transformation(spark, dataframe):
    # create tempView for source table from sql query
    dataframe.createOrReplaceTempView("fct_game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("bailed_games") \
        .getOrCreate()
    output_df = do_bailed_games_transformation(spark, spark.table("fct_game_details"))
    # insert into bailed_games
    output_df.write.mode("overwrite").insertInto("bailed_games")
    