from chispa.dataframe_comparer import *

from ..jobs.team_vertex_job import do_team_vertex_transformation
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")


def test_vertex_generation(spark):
    input_data = [
        Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_team_vertex_transformation(spark, input_dataframe)
    expected_output = [
        TeamVertex(
            identifier=1,
            type='team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    examples
    of
    jobs and tests:
    from pyspark.sql import SparkSession

    def do_monthly_user_site_hits_transformation(spark, dataframe, ds):
        query = f"""
        SELECT
               month_start,
               SUM(COALESCE(hit_array[0], 0)) as num_hits_first_day,
               SUM(COALESCE(hit_array[1], 0)) AS num_hits_second_day,
               SUM(COALESCE(hit_array[2], 0)) as num_hits_third_day
        FROM monthly_user_site_hits
        WHERE date_partition = '{ds}'
        GROUP BY month_start
        """
        dataframe.createOrReplaceTempView("monthly_user_site_hits")
        return spark.sql(query)

    def main():
        ds = '2023-01-01'
        spark = SparkSession.builder \
            .master("local") \
            .appName("players_scd") \
            .getOrCreate()
        output_df = do_monthly_user_site_hits_transformation(spark, spark.table("monthly_user_site_hits"), ds)
        output_df.write.mode("overwrite").insertInto("monthly_user_site_hits_agg")
        from chispa.dataframe_comparer import *

    from ..jobs.monthly_user_site_hits_job import do_monthly_user_site_hits_transformation
    from collections import namedtuple

    MonthlySiteHit = namedtuple("MonthlySiteHit", "month_start hit_array date_partition")
    MonthlySiteHitsAgg = namedtuple("MonthlySiteHitsAgg",
                                    "month_start num_hits_first_day num_hits_second_day num_hits_third_day")

    def test_monthly_site_hits(spark):
        ds = "2023-03-01"
        new_month_start = "2023-04-01"
        input_data = [
            # Make sure basic case is handled gracefully
            MonthlySiteHit(
                month_start=ds,
                hit_array=[0, 1, 3],
                date_partition=ds
            ),
            MonthlySiteHit(
                month_start=ds,
                hit_array=[1, 2, 3],
                date_partition=ds
            ),
            #  Make sure empty array is handled gracefully
            MonthlySiteHit(
                month_start=new_month_start,
                hit_array=[],
                date_partition=ds
            ),
            # Make sure other partitions get filtered
            MonthlySiteHit(
                month_start=new_month_start,
                hit_array=[],
                date_partition=""
            )
        ]

        source_df = spark.createDataFrame(input_data)
        actual_df = do_monthly_user_site_hits_transformation(spark, source_df, ds)

        expected_values = [
            MonthlySiteHitsAgg(
                month_start=ds,
                num_hits_first_day=1,
                num_hits_second_day=3,
                num_hits_third_day=6
            ),
            MonthlySiteHitsAgg(
                month_start=new_month_start,
                num_hits_first_day=0,
                num_hits_second_day=0,
                num_hits_third_day=0
            )
        ]
        expected_df = spark.createDataFrame(expected_values)
        assert_df_equality(actual_df, expected_df)

    from pyspark.sql import SparkSession

    query = """

    WITH streak_started AS (
        SELECT player_name,
               current_season,
               scoring_class,
               LAG(scoring_class, 1) OVER
                   (PARTITION BY player_name ORDER BY current_season) <> scoring_class       
                   OR LAG(scoring_class, 1) OVER
                   (PARTITION BY player_name ORDER BY current_season) IS NULL
                   AS did_change
        FROM players
    ),
         streak_identified AS (
             SELECT
                player_name,
                    scoring_class,
                    current_season,
                SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                    OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
             FROM streak_started
         ),
         aggregated AS (
             SELECT
                player_name,
                scoring_class,
                streak_identifier,
                MIN(current_season) AS start_date,
                MAX(current_season) AS end_date
             FROM streak_identified
             GROUP BY 1,2,3
         )

         SELECT player_name, scoring_class, start_date, end_date
         FROM aggregated

    """

    def do_player_scd_transformation(spark, dataframe):
        dataframe.createOrReplaceTempView("players")
        return spark.sql(query)

    def main():
        spark = SparkSession.builder \
            .master("local") \
            .appName("players_scd") \
            .getOrCreate()
        output_df = do_player_scd_transformation(spark, spark.table("players"))
        output_df.write.mode("overwrite").insertInto("players_scd")

    from chispa.dataframe_comparer import *
    from ..jobs.players_scd_job import do_player_scd_transformation
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

        actual_df = do_player_scd_transformation(spark, source_df)
        expected_data = [
            PlayerScd("Michael Jordan", 'Good', 2001, 2002),
            PlayerScd("Michael Jordan", 'Bad', 2003, 2003),
            PlayerScd("Someone Else", 'Bad', 2003, 2003)
        ]
        expected_df = spark.createDataFrame(expected_data)
        assert_df_equality(actual_df, expected_df)
        from pyspark.sql import SparkSession

    query = """

    WITH teams_deduped AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
        FROM teams
    )
    SELECT
        team_id AS identifier,
        'team' AS `type`,
        map(
            'abbreviation', abbreviation,
            'nickname', nickname,
            'city', city,
            'arena', arena,
            'year_founded', CAST(yearfounded AS STRING)
            ) AS properties
    FROM teams_deduped
    WHERE row_num = 1

    """

    def do_team_vertex_transformation(spark, dataframe):
        dataframe.createOrReplaceTempView("teams")
        return spark.sql(query)

    def main():
        spark = SparkSession.builder \
            .master("local") \
            .appName("players_scd") \
            .getOrCreate()
        output_df = do_team_vertex_transformation(spark, spark.table("players"))
        output_df.write.mode("overwrite").insertInto("players_scd")
        from chispa.dataframe_comparer import *

    from ..jobs.team_vertex_job import do_team_vertex_transformation
    from collections import namedtuple

    TeamVertex = namedtuple("TeamVertex", "identifier type properties")
    Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

    def test_vertex_generation(spark):
        input_data = [
            Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
            Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900),
        ]

        input_dataframe = spark.createDataFrame(input_data)
        actual_df = do_team_vertex_transformation(spark, input_dataframe)
        expected_output = [
            TeamVertex(
                identifier=1,
                type='team',
                properties={
                    'abbreviation': 'GSW',
                    'nickname': 'Warriors',
                    'city': 'San Francisco',
                    'arena': 'Chase Center',
                    'year_founded': '1900'
                }
            )
        ]
        expected_df = spark.createDataFrame(expected_output)
        assert_df_equality(actual_df, expected_df, ignore_nullable=True)

