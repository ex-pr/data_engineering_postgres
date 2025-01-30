from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, coalesce, date_trunc, array, lit

def do_daily_aggregation(spark, dataframe, ds):
    # Register the input dataframe as a temporary view
    dataframe.createOrReplaceTempView("events")

    # Define the query for daily aggregation
    query = f"""
    WITH daily_aggregate AS (
        SELECT user_id,
               CAST(event_time AS DATE) AS date,
               COUNT(1) AS num_site_hits
        FROM events
        WHERE CAST(event_time AS DATE) = CAST('{ds}' AS DATE)
          AND user_id IS NOT NULL
        GROUP BY user_id, CAST(event_time AS DATE)
    ),
    yesterday_array AS (
        SELECT *
        FROM array_metrics
        WHERE month_start = DATE_TRUNC('month', CAST('{ds}' AS DATE))
    )
    SELECT COALESCE(da.user_id, ya.user_id) AS user_id,
           COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
           'site_hits' AS metric_name,
           CASE
               WHEN ya.metric_array IS NOT NULL THEN concat(ya.metric_array, ARRAY(COALESCE(da.num_site_hits, 0)))
               WHEN ya.metric_array IS NULL THEN concat(
                   TRANSFORM(
                       SEQUENCE(1, COALESCE(DAYOFMONTH(da.date) - 1, 0)),
                       x -> 0
                   ),
                   ARRAY(COALESCE(da.num_site_hits, 0))
               )
           END AS metric_array
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya ON da.user_id = ya.user_id
    """
    return spark.sql(query)

def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
        .master("local") \
        .appName("daily_aggregation") \
        .getOrCreate()
    output_df = do_daily_aggregation(spark, spark.table("events"), ds)
    output_df.write.mode("overwrite").insertInto("array_metrics")