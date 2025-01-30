from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from ..jobs.user_activity_analysis import do_user_activity_analysis
from collections import namedtuple

# Define namedtuples for input and output data
UserCumulated = namedtuple("UserCumulated", "user_id dates_active date")
UserActivityMetrics = namedtuple("UserActivityMetrics", "user_id datelist_int monthly_active l32 weekly_active l7 weekly_active_previous_week")

def test_user_activity_analysis(spark):
    # Create fake input data
    input_data = [
        UserCumulated(1, ["2023-03-01", "2023-03-15"], "2023-03-31"),
        UserCumulated(2, ["2023-03-10", "2023-03-20"], "2023-03-31")
    ]
    input_df = spark.createDataFrame(input_data)

    # Run the job
    ds = "2023-03-31"
    actual_df = do_user_activity_analysis(spark, input_df, ds)

    # Define expected output
    expected_data = [
        UserActivityMetrics(1, 1073741824, True, 2, True, 1, False),
        UserActivityMetrics(2, 1073741824, True, 2, True, 1, False)
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Assert equality
    assert_df_equality(actual_df, expected_df)