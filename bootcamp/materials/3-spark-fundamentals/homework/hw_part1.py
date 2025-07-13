#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col, broadcast, split, lit
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

#Disabled automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


# read data from csv into dataframe

match_details = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/match_details.csv")

matches = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/matches.csv")

medals_matches_players = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals_matches_players.csv")

medals = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals.csv")

maps = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/maps.csv")


# show data from df
match_details.show(1)
matches.show(1)
medals_matches_players.show(1)
medals.show(1)
maps.show(1)

# Explicitly broadcast JOINs medals and maps
# medals
medals_matches_players_joined_medals = medals_matches_players.alias("mmp") \
    .join(broadcast(medals).alias("me"), col("mmp.medal_id") == col("me.medal_id")) \
    .select("mmp.match_id", "mmp.player_gamertag", "mmp.count", "me.*") 

medals_matches_players_joined_medals.show(1)

# maps
matches_joined_maps = matches.alias("m") \
    .join(broadcast(maps).alias("mp"), col("m.mapid") == col("mp.mapid")) \
    .select("m.*", col("mp.name").alias("map_name"), col("description").alias("map_description"))

matches_joined_maps.show(1)


medals_matches_players_joined_medals.printSchema()


# match_details

# create iceberg table match_details_bucketed
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id string, 
     player_gamertag string ,
     previous_spartan_rank integer ,
     spartan_rank integer ,
     previous_total_xp integer ,
     total_xp integer ,
     previous_csr_tier integer ,
     previous_csr_designation integer ,
     previous_csr integer ,
     previous_csr_percent_to_next_tier integer ,
     previous_csr_rank integer ,
     current_csr_tier integer,
     current_csr_designation integer ,
     current_csr integer ,
     current_csr_percent_to_next_tier integer ,
     current_csr_rank integer ,
     player_rank_on_team integer ,
     player_finished boolean ,
     player_average_life string ,
     player_total_kills integer ,
     player_total_headshots integer ,
     player_total_weapon_damage double ,
     player_total_shots_landed integer,
     player_total_melee_kills integer ,
     player_total_melee_damage double ,
     player_total_assassinations integer ,
     player_total_ground_pound_kills integer ,
     player_total_shoulder_bash_kills integer ,
     player_total_grenade_damage double ,
     player_total_power_weapon_damage double ,
     player_total_power_weapon_grabs integer ,
     player_total_deaths integer ,
     player_total_assists integer ,
     player_total_grenade_kills integer ,
     did_win integer ,
     team_id integer 
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """)

# insert into match_details_bucketed from df match_details
match_details.writeTo("bootcamp.match_details_bucketed") \
    .append()



# matches_joined_maps

# create iceberg table matches_bucketed
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
     match_id string ,
     mapid string ,
     is_team_game boolean ,
     playlist_id string ,
     game_variant_id string ,
     is_match_over boolean ,
     completion_date timestamp ,
     match_duration string ,
     game_mode string ,
     map_variant_id string ,
     map_name string ,
     map_description string 
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """)

# insert into matches_bucketed from df matches_joined_maps
matches_joined_maps.writeTo("bootcamp.matches_bucketed") \
    .append()


# medals_matches_players_joined_medals

# create iceberg table medal_matches_players_bucketed
spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
     match_id string ,
     player_gamertag string ,
     count integer ,
     medal_id long ,
     sprite_uri string ,
     sprite_left integer ,
     sprite_top integer ,
     sprite_sheet_width integer ,
     sprite_sheet_height integer ,
     sprite_width integer ,
     sprite_height integer ,
     classification string ,
     description string ,
     name string ,
     difficulty integer 
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """)

# insert into medal_matches_players_bucketed from df medals_matches_players_joined_medals
medals_matches_players_joined_medals.writeTo("bootcamp.medal_matches_players_bucketed") \
    .append()



# Bucket join match_details, matches_joined_maps, and medals_matches_players_joined_medals on match_id with 16 buckets
# join data from iceberg tables 
joined_df = spark.sql("""
    SELECT mdb.match_id,
        mdb.player_gamertag,
        mdb.previous_spartan_rank,
        mdb.spartan_rank,
        mdb.previous_total_xp,
        mdb.total_xp,
        mdb.previous_csr_tier,
        mdb.previous_csr_designation,
        mdb.previous_csr,
        mdb.previous_csr_percent_to_next_tier,
        mdb.previous_csr_rank,
        mdb.current_csr_tier,
        mdb.current_csr_designation,
        mdb.current_csr,
        mdb.current_csr_percent_to_next_tier,
        mdb.current_csr_rank,
        mdb.player_rank_on_team,
        mdb.player_finished,
        mdb.player_average_life,
        mdb.player_total_kills,
        mdb.player_total_headshots,
        mdb.player_total_weapon_damage,
        mdb.player_total_shots_landed,
        mdb.player_total_melee_kills,
        mdb.player_total_melee_damage,
        mdb.player_total_assassinations,
        mdb.player_total_ground_pound_kills,
        mdb.player_total_shoulder_bash_kills,
        mdb.player_total_grenade_damage,
        mdb.player_total_power_weapon_damage,
        mdb.player_total_power_weapon_grabs,
        mdb.player_total_deaths,
        mdb.player_total_assists,
        mdb.player_total_grenade_kills,
        mdb.did_win,
        mdb.team_id,

        mb.mapid,
        mb.is_team_game,
        mb.playlist_id,
        mb.game_variant_id,
        mb.is_match_over,
        mb.completion_date,
        mb.match_duration,
        mb.game_mode,
        mb.map_variant_id,
        mb.map_name,
        mb.map_description,

        mmp.count,
        mmp.medal_id,
        mmp.sprite_uri,
        mmp.sprite_left,
        mmp.sprite_top,
        mmp.sprite_sheet_width,
        mmp.sprite_sheet_height,
        mmp.sprite_width,
        mmp.sprite_height,
        mmp.classification,
        mmp.description,
        mmp.name,
        mmp.difficulty
    FROM bootcamp.match_details_bucketed mdb 
    JOIN bootcamp.matches_bucketed mb 
        ON mdb.match_id = mb.match_id
    JOIN bootcamp.medal_matches_players_bucketed mmp 
        ON mdb.match_id = mmp.match_id AND mdb.player_gamertag = mmp.player_gamertag  
""")


joined_df.explain()



joined_df.printSchema()


# Aggregate the joined data frame to figure out questions like:


# Which player averages the most kills per game?

most_kills_per_game = joined_df.groupBy("player_gamertag") \
    .agg(F.avg("player_total_kills").alias("avg_kills_per_game")) \
    .orderBy(F.desc("avg_kills_per_game"))


most_kills_per_game.show(1)



# Which playlist gets played the most?

most_played_playlists = joined_df.select("match_id", "playlist_id").dropDuplicates() \
    .groupBy("playlist_id") \
    .agg(F.count("*").alias("total_matches")) \
    .orderBy(F.desc("total_matches"))

most_played_playlists.show(1)



# Which map gets played the most?

most_played_maps = joined_df.select("match_id", "map_name").dropDuplicates() \
    .groupBy("map_name") \
    .agg(F.count("*").alias("total_matches")) \
    .orderBy(F.desc("total_matches"))

most_played_maps.show(1)



# Which map do players get the most Killing Spree medals on?

killing_spree_by_map = joined_df.filter(F.col("name") == "Killing Spree") \
    .groupBy("map_name") \
    .agg(F.sum("count").alias("total_killing_spree_medals")) \
    .orderBy(F.desc("total_killing_spree_medals"))

killing_spree_by_map.show(1)


# Try different .sortWithinPartitions to see which has the smallest data size

playlist_sort_df = joined_df.sortWithinPartitions(col("playlist_id"))
playlist_sort_df.write.mode("overwrite").saveAsTable("bootcamp.df_sorted_playlist")

map_sort_df = joined_df.sortWithinPartitions(col("map_name"))
map_sort_df.write.mode("overwrite").saveAsTable("bootcamp.df_sorted_map")


query = """
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'df_sorted_playlist' as table_name
FROM demo.bootcamp.df_sorted_playlist.files

UNION ALL

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'df_sorted_map' as table_name
FROM demo.bootcamp.df_sorted_map.files
"""

result_df = spark.sql(query)
result_df.show()



