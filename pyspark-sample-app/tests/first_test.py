import pytest
from gill.spark import get_spark
from gill.mission import with_life_goal

class TestMission(object):

    def my_first_function(self):
        source_data = [
            ("simple", 1),
            ("complex", 0)
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["level", "time"]
        )

        actual_df = with_life_goal(source_df)

        expected_data = [
            ("simple", 1, "escape!"),
            ("comlex", 0, "escape!")
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["level", "time", "func"]
        )

        assert(expected_df.collect() == actual_df.collect())
