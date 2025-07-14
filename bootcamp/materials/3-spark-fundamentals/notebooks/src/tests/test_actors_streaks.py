from chispa.dataframe_comparer import *

# add job name, used for this test, and function inside it
from ..jobs.actors_streaks_job import do_actors_streaks_transformation
from collections import namedtuple

#source schema
Actors = namedtuple("Actors", "actorid current_year quality_class is_active")
#target schema
ActorsScd = namedtuple("ActorsScd", "actorid quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    source_data = [
        Actors("123", 1976, 'good', True),
        Actors("123", 1977, 'good', True),
        Actors("123", 1978, 'star', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_streaks_transformation(spark, source_df)
    expected_data = [
        ActorsScd("123", 'good', True, 1976, 1977, 1978),
        ActorsScd("123", 'star', True, 1978, 1978, 1978)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)