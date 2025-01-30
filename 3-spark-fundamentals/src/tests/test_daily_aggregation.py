from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from ..jobs.daily_aggregation import do_daily_aggregation
from collections import namedtuple

# Define namedtuples for input and output data
Event = namedtuple("Event", "user_id event_time")
ArrayMetric = namedtuple("ArrayMetric", "user_id month_start metric_name metric_array")

def test_daily_aggregation(spark):
    # Create fake input data
    events_data = [
        Event(1, "2023-01-01 10:00:00"),
        Event(2, "2023-01-01 11:00:00")
    ]
    events_df = spark.createDataFrame(events_data)

    array_metrics_data = [
        ArrayMetric(1, "2023-01-01", "site_hits", [0, 0])
    ]
    array_metrics_df = spark.createDataFrame(array_metrics_data)

    # Register input tables
    events_df.createOrReplaceTempView("events")
    array_metrics_df.createOrReplaceTempView("array_metrics")

    # Run the job
    ds = "2023-01-01"
    actual_df = do_daily_aggregation(spark, events_df, ds)

    # Define expected output
    expected_data = [
        ArrayMetric(1, "2023-01-01", "site_hits", [0, 0, 1]),
        ArrayMetric(2, "2023-01-01", "site_hits", [0, 1])
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Assert equality
    assert_df_equality(actual_df, expected_df)