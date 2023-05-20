import pandas as pd
import os
import requests
from retry import retry
from datetime import datetime


class csgo_analyzer():
    LOCAL_CSV_PATH = "c:/projects/csgo_parser/csv"

    def __init__(self, match_id, match_date):
        self.match_id = match_id
        self.match_date = match_date
        print()

    def read_csv_to_pd(self):
        # Removing all dataframes from memory
        lst = [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
        del lst

        csv_files = os.listdir(self.LOCAL_CSV_PATH)
        self.dataframes = {}

        for file in csv_files:
            if file.endswith('.csv'):
                # Remove a extensão .csv do nome do arquivo
                nome_dataframe = file[:-4]
                caminho_arquivo = os.path.join(self.LOCAL_CSV_PATH, file)
                self.dataframes[nome_dataframe] = pd.read_csv(caminho_arquivo)

    def func_kda(self):

        # Tick of the match start (After the warmup)
        tick_round_start = str(self.dataframes["round_announce_match_start"][
            "tick"][0])

        # How many teamate kills
        df_player_sides = self.dataframes["parse_players"][["steamid", "starting_side"]]\
            .rename(columns={"steamid": "steamid_sides"})

        self.dataframes["player_death"] = pd.merge(self.dataframes["player_death"],
                                                   df_player_sides,
                                                   how='left',
                                                   left_on=[
                                                       'attacker_steamid'],
                                                   right_on=['steamid_sides'])\
            .drop(columns=['steamid_sides'])\
            .rename(columns={"starting_side": "attacker_side"})
        self.dataframes["player_death"] = pd.merge(self.dataframes["player_death"],
                                                   df_player_sides,
                                                   how='left',
                                                   left_on=['player_steamid'],
                                                   right_on=['steamid_sides'])\
            .drop(columns=['steamid_sides'])\
            .rename(columns={"starting_side": "player_side"})

        # Calculating the Kill number for each player
        df_kda = self.dataframes["parse_players"][[
            'steamid', 'name', 'user_id']]

        df_kills = self.dataframes["player_death"]\
            .query("attacker_steamid != 0")\
            .query("tick > "+tick_round_start)\
            .query("attacker_side != "+tick_round_start)\
            .groupby('attacker_steamid')["attacker_steamid"]\
            .count().reset_index(name="kills")

        df_kill_teammates = self.dataframes["player_death"]\
            .query("attacker_side == player_side")\
            .query("tick > "+tick_round_start)\
            .groupby('attacker_steamid')["attacker_steamid"]\
            .count().reset_index(name="kill_teammates")\
            .rename(columns={'attacker_steamid': 'attacker_steamid_teammates'})

        df_kills = pd.merge(df_kills, df_kill_teammates, how='left', left_on=[
            'attacker_steamid'], right_on=['attacker_steamid_teammates'])\
            .drop(columns=['attacker_steamid_teammates'])\
            .fillna(0)

        df_kills["true_kills"] = (df_kills["kills"]
                                  - 2 * df_kills["kill_teammates"])

        df_kills = df_kills.drop(columns=['kills'])\
            .rename(columns={"true_kills": "kills"})

        df_deaths = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .groupby('player_steamid')["player_steamid"]\
            .count().reset_index(name="death")

        df_assist = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .query("assister !=0")\
            .groupby('assister')["assister"]\
            .count().reset_index(name="assist")

        df_kda = pd.merge(df_kda, df_kills, how='left', left_on=[
                          'steamid'], right_on=['attacker_steamid'])\
            .drop(columns=['attacker_steamid'])
        df_kda = pd.merge(df_kda, df_deaths, how='left', left_on=[
                          'steamid'], right_on=['player_steamid'])\
            .drop(columns=['player_steamid'])
        df_kda = pd.merge(df_kda, df_assist, how='left', left_on=[
                          'user_id'], right_on=['assister'])\
            .drop(columns=['assister'])

        self.export_to_json(df_kda)

        print()

    def get_match_map(self):
        match_map = str(self.dataframes["parse_header"]["map_name"][0])
        return match_map

    def get_score_first_half(self):
        """
        Return how many round each side won in the first half.
        3 = CT
        2 = T
        """
        df_score_first = self.dataframes["round_end"][["winner"]].iloc[:15]
        df_score_first = df_score_first\
            .groupby("winner")["winner"]\
            .count().reset_index(name="rounds")
        df_score_first["winner"] = df_score_first["winner"]\
            .map({3: "ct",
                  2: "t"})
        df_score_first = df_score_first.rename(
            columns={"winner": "winner_starting_side"})

        return df_score_first.to_json(orient='records', indent=4)

    def get_score_second_half(self):
        """
        Return how many round each side won in the second half
        NOTE! For the second half we will switch sides.
        If CT wins a round, we will put it as a T because we count
        By the 'Starting side' of the team.
        """
        df_score_second = self.dataframes["round_end"][["winner"]].iloc[15:]
        df_score_second = df_score_second\
            .groupby("winner")["winner"]\
            .count().reset_index(name="rounds")
        df_score_second["winner"] = df_score_second["winner"]\
            .map({2: "ct",
                  3: "t"})
        df_score_second = df_score_second.rename(
            columns={"winner": "winner_starting_side"})
        print()

        return df_score_second.to_json(orient='records', indent=4)

    @retry(Exception, tries=3, delay=1)
    def export_to_json(self, df):
        # Converting the df to a JSON
        json_data = df.to_json(orient='records', indent=4)

        # Get the map of the match
        match_map = self.get_match_map()

        # Get the score of CT
        score_first_half = self.get_score_first_half()

        # Get the score of T
        score_second_half = self.get_score_second_half()

        # Creating the header of the object
        data_dict = {
            "match_id": self.match_id,
            "score_first_half": score_first_half,
            "score_second_half": score_second_half,
            "match_map": match_map,
            "match_date": self.match_id,
            "data": json_data
        }

        # Set the REST endpoint URL
        url = 'https://xaxanalytics/kda'

        # Set the request headers (if needed)
        headers = {'Content-Type': 'application/json'}

        # Make the HTTP POST request
        # response = requests.post(url, data=data_dict, headers=headers)

        # Check the response status code
        # if response.status_code == 200:
        #     print('Data sent successfully to the REST endpoint.')
        #     return
        # else:
        #     print('Failed to send data to the REST endpoint. Status code:',
        #           response.status_code)

    def main(self):
        self.read_csv_to_pd()
        self.func_kda()
        print()
