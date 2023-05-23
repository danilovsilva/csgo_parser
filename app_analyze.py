import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StringType
from retry import retry


class CSGOAnalyzer:
    LOCAL_CSV_PATH = "c:/projects/csgo_parser/csv"

    def __init__(self, match_id, match_date):
        self.match_id = match_id
        self.match_date = match_date

    def read_csv_to_pyspark(self):
        spark = SparkSession.builder.getOrCreate()
        csv_files = os.listdir(self.LOCAL_CSV_PATH)
        self.dataframes = {}

        for file in csv_files:
            if file.endswith('.csv'):
                # Remove a extensão .csv do nome do arquivo
                nome_dataframe = file[:-4]
                caminho_arquivo = os.path.join(self.LOCAL_CSV_PATH, file)
                self.dataframes[nome_dataframe] = spark.read.csv(
                    caminho_arquivo, header=True)

    def func_kda(self):
        spark = SparkSession.builder.getOrCreate()

        player_death_df = self.dataframes["player_death"]
        round_announce_match_start_df = self.dataframes["round_announce_match_start"]
        parse_players_df = self.dataframes["parse_players"]

        # Tick of the match start (After the warmup)
        tick_round_start = str(
            round_announce_match_start_df.select("tick").first()["tick"])

        df_player_sides = parse_players_df.select("steamid", "starting_side") \
            .withColumnRenamed("steamid", "steamid_sides")

        player_death_df = player_death_df.join(df_player_sides, player_death_df["attacker_steamid"] == df_player_sides["steamid_sides"], "left") \
            .drop("steamid_sides") \
            .withColumnRenamed("starting_side", "attacker_side")

        player_death_df = player_death_df.join(df_player_sides, player_death_df["player_steamid"] == df_player_sides["steamid_sides"], "left") \
            .drop("steamid_sides") \
            .withColumnRenamed("starting_side", "player_side")

        df_kda = parse_players_df.select('steamid', 'name', 'user_id')

        df_kills = (player_death_df
                    .filter((col("attacker_steamid") != 0) &
                            (col("tick") > tick_round_start) &
                            (col("attacker_side") != tick_round_start))
                    .groupBy("attacker_steamid")
                    .agg(count("attacker_steamid").alias("kills")))

        df_kill_teammates = (player_death_df
                             .filter((col("attacker_side") == col("player_side")) &
                                     (col("tick") > tick_round_start))
                             .groupBy("attacker_steamid")
                             .agg(count("attacker_steamid").alias("kill_teammates"))
                             .withColumnRenamed("attacker_steamid", "attacker_steamid_teammates"))

        df_kills = df_kills.join(df_kill_teammates, df_kills["attacker_steamid"] == df_kill_teammates["attacker_steamid_teammates"], "left") \
            .drop("attacker_steamid_teammates") \
            .fillna(0)

        df_kills = df_kills.withColumn(
            "true_kills", col("kills") - 2 * col("kill_teammates"))

        df_kills = df_kills.drop("kills") \
            .withColumnRenamed("true_kills", "kills")

        df_deaths = (player_death_df
                     .filter(col("tick") > tick_round_start)
                     .groupBy("player_steamid")
                     .agg(count("player_steamid").alias("death")))

        df_assist = (player_death_df
                     .filter((col("tick") > tick_round_start) & (col("assister") != 0))
                     .groupBy("assister")
                     .agg(count("assister").alias("assist")))

        df_kda = df_kda.join(df_kills, df_kda["steamid"] == df_kills["attacker_steamid"], "left") \
            .drop("attacker_steamid")
        df_kda = df_kda.join(df_deaths, df_kda["steamid"] == df_deaths["player_steamid"], "left") \
            .drop("player_steamid")
        df_kda = df_kda.join(df_assist, df_kda["user_id"] == df_assist["assister"], "left") \
            .drop("assister")

        self.export_to_json(df_kda)

    def get_match_map(self):
        match_map = str(self.dataframes["parse_header"].select(
            "map_name").first()["map_name"])
        return match_map

    def get_score_first_half(self):
        """
        Return how many round each side won in the first half.
        3 = CT
        2 = T
        """
        df_score_first = self.dataframes["round_end"].select(
            "winner").limit(15)
        df_score_first = df_score_first \
            .groupBy("winner") \
            .agg(count("winner").alias("rounds"))
        df_score_first = df_score_first.withColumn("winner_starting_side",
                                                   col("winner").cast(
                                                       StringType())
                                                   .replace("3", "ct")
                                                   .replace("2", "t"))
        return df_score_first.toJSON().collect()

    def get_score_second_half(self):
        """
        Return how many round each side won in the second half
        NOTE! For the second half we will switch sides.
        If CT wins a round, we will put it as a T because we count
        By the 'Starting side' of the team.
        """
        df_score_second = self.dataframes["round_end"].select(
            "winner").offset(15)
        df_score_second = df_score_second \
            .groupBy("winner") \
            .agg(count("winner").alias("rounds"))
        df_score_second = df_score_second.withColumn("winner_starting_side",
                                                     col("winner").cast(
                                                         StringType())
                                                     .replace("2", "ct")
                                                     .replace("3", "t"))
        return df_score_second.toJSON().collect()

    @retry(Exception, tries=3, delay=1)
    def export_to_json(self, df):
        """
        Export DataFrame to JSON and send it to a REST endpoint.

        Args:
            df (DataFrame): The DataFrame to export.

        Raises:
            Exception: If sending data to the REST endpoint fails.
        """
        spark = SparkSession.builder.getOrCreate()
        json_data = df.toJSON().collect()

        match_map = self.get_match_map()

        score_first_half = self.get_score_first_half()

        score_second_half = self.get_score_second_half()

        data_dict = {
            "match_id": self.match_id,
            "score_first_half": score_first_half,
            "score_second_half": score_second_half,
            "match_map": match_map,
            "match_date": self.match_id,
            "data": json_data
        }

        json_file = spark.createDataFrame(
            [json.dumps(data_dict)], StringType())

        json_file.write.mode("overwrite").text(
            "c:/projects/csgo_parser/output")

    def main(self):
        self.read_csv_to_pd()
        self.func_kda()
        print()
