import pandas as pd
import os
import numpy as np
# import requests
from retry import retry
from datetime import datetime


class csgo_analyzer():
    LOCAL_CSV_PATH = "c:/projects/csgo_parser/csv"

    def __init__(self, match_id):
        self.match_id = match_id
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
        df_player_sides = self.dataframes["parse_players"]\
            [["steamid", "starting_side"]]\
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

        df_headshot_kills = self.dataframes["player_death"]\
            .query("attacker_steamid != 0")\
            .query("tick > "+tick_round_start)\
            .query("attacker_side != "+tick_round_start)\
            .query("headshot == True")\
            .groupby('attacker_steamid')["attacker_steamid"]\
            .count().reset_index(name="headshot_kills")\
            .rename(columns={"attacker_steamid": "attacker_steamid_headshot_kills"})

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

        df_kills = pd.merge(df_kills, df_headshot_kills, how='left', left_on=[
            'attacker_steamid'], right_on=['attacker_steamid_headshot_kills'])\
                .drop(columns=['attacker_steamid_headshot_kills'])\
                .fillna(0)

        df_kills["true_kills"] = (df_kills["kills"] \
                                  - 2 * df_kills["kill_teammates"])

        df_kills = df_kills.drop(columns=['kills'])\
            .rename(columns={"true_kills": "kills"})

        df_deaths = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .groupby('player_steamid')["player_steamid"]\
            .count().reset_index(name="death")

        df_headshot_deaths = self.dataframes["player_death"]\
            .query("attacker_steamid != 0")\
            .query("tick > "+tick_round_start)\
            .query("attacker_side != "+tick_round_start)\
            .query("headshot == True")\
            .groupby('player_steamid')["player_steamid"]\
            .count().reset_index(name="headshot_deaths")\
            .rename(columns={"player_steamid": "player_steamid_headshot_deaths"})

        df_deaths = pd.merge(df_deaths, df_headshot_deaths, how='left', left_on=[
            'player_steamid'], right_on=['player_steamid_headshot_deaths'])\
                .drop(columns=['player_steamid_headshot_deaths'])\
                .fillna(0)

        df_assist = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .query("assister !=0")\
            .groupby('assister')["assister"]\
            .count().reset_index(name="assist")
        
        df_weapon_kills = self.dataframes["player_death"][["attacker_steamid", "weapon", "tick", "attacker_side"]]\
            .query("tick > " + tick_round_start)\
            .query("attacker_side != "+tick_round_start)\
            .groupby(['attacker_steamid', 'weapon'])['attacker_steamid']\
            .count().reset_index(name="kills")
        
        df_weapon_kills_pivoted = df_weapon_kills.pivot_table(index='attacker_steamid', columns='weapon', values='kills', fill_value=0)

        df_weapon_deaths = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .groupby(['player_steamid', 'weapon'])["player_steamid"]\
            .count().reset_index(name="deaths")
        
        df_weapon_deaths_pivoted = df_weapon_deaths.pivot_table(index='player_steamid', columns='weapon', values='deaths', fill_value=0)

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
        print()
        return df_score_first

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
        return df_score_second

    def get_total_damage_health(self):

        # Tick of the match start (After the warmup)
        tick_round_start = str(self.dataframes["round_announce_match_start"][
            "tick"][0])
        df_dmg_health = self.dataframes["player_hurt"]

        df_dmg_health["tot_dmg_health"] = np.where(df_dmg_health["dmg_health"]>100,100,df_dmg_health["dmg_health"])

        df_dmg_health = df_dmg_health.query("attacker_steamid != 0")\
            .query("tick > "+tick_round_start)\
            .groupby('attacker_steamid')["tot_dmg_health"]\
            .sum().reset_index(name="tot_dmg_health")

        self.export_to_json(df_dmg_health)

    def get_total_damage_armor(self):

        # Tick of the match start (After the warmup)
        tick_round_start = str(self.dataframes["round_announce_match_start"][
            "tick"][0])
        df_dmg_armor = self.dataframes["player_hurt"]

        df_dmg_armor["tot_dmg_armor"] = np.where(df_dmg_armor["dmg_armor"]>100,100,df_dmg_armor["dmg_armor"])

        df_dmg_armor = df_dmg_armor.query("attacker_steamid != 0")\
            .query("tick > "+tick_round_start)\
            .groupby('attacker_steamid')["tot_dmg_armor"]\
            .sum().reset_index(name="tot_dmg_armor")

        self.export_to_json(df_dmg_armor)


    @retry(Exception, tries=3, delay=1)
    def export_to_json(self, df):
        # Converting the df to a JSON
        json_data = df.to_json(orient='records', indent=4)

        # Get the map of the match
        match_map = self.get_match_map()

        # Get the score of CT
        score_ct = self.get_score_first_half()

        # Get the score of T
        score_t = self.get_score_second_half()

        # Creating the header of the object
        data_dict = {
            "match_id": self.match_id,
            "score_ct": score_ct,
            "score_tr": score_t,
            "match_map": match_map,
            "match_date": datetime.today().strftime('%Y-%m-%d'),
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
        self.get_total_damage_health()
        self.get_total_damage_armor()
        print()
