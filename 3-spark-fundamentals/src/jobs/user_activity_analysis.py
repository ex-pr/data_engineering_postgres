from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as _sum, when

def do_user_activity_analysis(spark, dataframe, ds):
    # Register the input dataframe as a temporary view
    dataframe.createOrReplaceTempView("users_cumulated")

    # Define the query for user activity analysis
    query = f"""
    WITH starter AS (
        SELECT array_contains(uc.dates_active, CAST(d.valid_date AS DATE)) AS is_active,
               DATEDIFF(CAST('{ds}' AS DATE), CAST(d.valid_date AS DATE)) AS days_since,
               uc.user_id
        FROM users_cumulated uc
        CROSS JOIN (
            SELECT explode(sequence(CAST('2023-02-28' AS DATE), CAST('{ds}' AS DATE), INTERVAL 1 DAY)) AS valid_date
        ) d
        WHERE uc.date = CAST('{ds}' AS DATE)
    ),
    bits AS (
        SELECT user_id,
               SUM(CASE WHEN is_active THEN POW(2, 31 - days_since) ELSE 0 END) AS datelist_int
        FROM starter
        GROUP BY user_id
    )
    SELECT user_id,
           datelist_int,
           BIT_COUNT(datelist_int) > 0 AS monthly_active,
           BIT_COUNT(datelist_int) AS l32,
           BIT_COUNT(datelist_int & CAST(0b11111110000000000000000000000000 AS INT)) > 0 AS weekly_active,
           BIT_COUNT(datelist_int & CAST(0b11111110000000000000000000000000 AS INT)) AS l7,
           BIT_COUNT(datelist_int & CAST(0b00000001111111000000000000000000 AS INT)) > 0 AS weekly_active_previous_week
    FROM bits
    """
    return spark.sql(query)

def main():
    ds = '2023-03-31'
    spark = SparkSession.builder \
        .master("local") \
        .appName("user_activity_analysis") \
        .getOrCreate()
    output_df = do_user_activity_analysis(spark, spark.table("users_cumulated"), ds)
    output_df.write.mode("overwrite").insertInto("user_activity_metrics")