from chispa.dataframe_comparer import *

# add job name, used for this test, and function inside it
from ..jobs.bailed_games_job import do_bailed_games_transformation
from collections import namedtuple

#source schema 
FctGameDetails = namedtuple("FctGameDetails", "dim_player_name dim_not_with_team")

#target schema
BailedGames = namedtuple("BailedGames", "dim_player_name num_games bailed_num bail_pct")


def test_games_count(spark):
    source_data = [
        FctGameDetails("John Smith", True),
        FctGameDetails("John Smith", True),
        FctGameDetails("John Smith", True),
        FctGameDetails("John Smith", False),
        FctGameDetails("Tom Hilton", True),
        FctGameDetails("Tom Hilton", False)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_bailed_games_transformation(spark, source_df)
    expected_data = [
        BailedGames("John Smith", 4, 3, 0.75),
        BailedGames("Tom Hilton", 2, 1, 0.5)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)