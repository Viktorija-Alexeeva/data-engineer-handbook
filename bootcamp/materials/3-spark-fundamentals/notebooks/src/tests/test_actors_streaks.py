from chispa.dataframe_comparer import *

# add job name, used for this test, and function inside it
from ..jobs.actors_streaks_job import do_actors_streaks_transformation
from collections import namedtuple

PlayerSeason = namedtuple("PlayerSeason", "player_name current_season scoring_class")
PlayerScd = namedtuple("PlayerScd", "player_name scoring_class start_date end_date")


def test_scd_generation(spark):
    source_data = [
        PlayerSeason("Michael Jordan", 2001, 'Good'),
        PlayerSeason("Michael Jordan", 2002, 'Good'),
        PlayerSeason("Michael Jordan", 2003, 'Bad'),
        PlayerSeason("Someone Else", 2003, 'Bad')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_streaks_transformation(spark, source_df)
    expected_data = [
        PlayerScd("Michael Jordan", 'Good', 2001, 2002),
        PlayerScd("Michael Jordan", 'Bad', 2003, 2003),
        PlayerScd("Someone Else", 'Bad', 2003, 2003)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)