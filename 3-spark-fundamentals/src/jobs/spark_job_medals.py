from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Match Analytics") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# Load datasets
match_details_path = "data/match_details.csv"
matches_path = "data/matches.csv"
medals_matches_players_path = "data/medals_matches_players.csv"
medals_path = "data/medals.csv"
maps_path = "data/maps.csv"

match_details = spark.read.csv(match_details_path, header=True, inferSchema=True)
matches = spark.read.csv(matches_path, header=True, inferSchema=True)
medals_matches_players = spark.read.csv(medals_matches_players_path, header=True, inferSchema=True)
medals = spark.read.csv(medals_path, header=True, inferSchema=True)
maps = spark.read.csv(maps_path, header=True, inferSchema=True)

# Data Validation
# Filter out rows with missing mapid in maps.csv
maps = maps.filter(col("mapid").isNotNull())

# Filter out rows with missing match_id or player_gamertag in match_details.csv
match_details = match_details.filter(col("match_id").isNotNull() & col("player_gamertag").isNotNull())

# Filter out rows with missing match_id or medal_id in medals_matches_players.csv
medals_matches_players = medals_matches_players.filter(col("match_id").isNotNull() & col("medal_id").isNotNull())

# Drop duplicates in medals_matches_players
medals_matches_players = medals_matches_players.dropDuplicates(["match_id", "player_gamertag", "medal_id"])

# Broadcast smaller datasets
broadcast_medals = broadcast(medals)
broadcast_maps = broadcast(maps)

# Bucket larger datasets using bucketBy() during write
match_details.write \
    .bucketBy(16, "match_id") \
    .sortBy("match_id") \
    .mode("overwrite") \
    .saveAsTable("match_details_bucketed")

matches.write \
    .bucketBy(16, "match_id") \
    .sortBy("match_id") \
    .mode("overwrite") \
    .saveAsTable("matches_bucketed")

medals_matches_players.write \
    .bucketBy(16, "match_id") \
    .sortBy("match_id") \
    .mode("overwrite") \
    .saveAsTable("medals_matches_players_bucketed")

# Load bucketed tables
match_details_bucketed = spark.table("match_details_bucketed")
matches_bucketed = spark.table("matches_bucketed")
medals_matches_players_bucketed = spark.table("medals_matches_players_bucketed")

# Join datasets
joined_df = match_details_bucketed.join(matches_bucketed, "match_id") \
    .join(medals_matches_players_bucketed, ["match_id", "player_gamertag"]) \
    .join(broadcast_medals, "medal_id") \
    .join(broadcast_maps, matches_bucketed.mapid == maps.mapid)

# Aggregations
# 1. Player with the most average kills per game
avg_kills_per_player = joined_df.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills_per_game")) \
    .orderBy(desc("avg_kills_per_game"))

# 2. Most played playlist
most_played_playlist = joined_df.groupBy("playlist_id") \
    .agg(count("*").alias("play_count")) \
    .orderBy(desc("play_count"))

# 3. Most played map
most_played_map = joined_df.groupBy("mapid", "name") \
    .agg(count("*").alias("map_count")) \
    .orderBy(desc("map_count"))

# 4. Map with the most Killing Spree medals
killing_spree_medals = joined_df.filter(col("name") == "Killing Spree") \
    .groupBy("mapid", "name") \
    .agg(count("*").alias("killing_spree_count")) \
    .orderBy(desc("killing_spree_count"))

# Save results locally
output_path = "output"

avg_kills_per_player.write.mode("overwrite").csv(f"{output_path}/avg_kills_per_player")
most_played_playlist.write.mode("overwrite").csv(f"{output_path}/most_played_playlist")
most_played_map.write.mode("overwrite").csv(f"{output_path}/most_played_map")
killing_spree_medals.write.mode("overwrite").csv(f"{output_path}/killing_spree_medals")

# Stop Spark session
spark.stop()