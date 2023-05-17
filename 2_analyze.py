import pandas as pd
import os


class csgo_analyzer():
    LOCAL_CSV_PATH = "c:/projects/csgo_parser/csv"

    def __init__(self):
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

        # fazer contagem após o tick do round_announce_match_start

        # fazer logica para remover a kill+1 quando matar miguxos
        # usar o starting side do parse_players

        # Tick of the match start (After the warmup)
        tick_round_start = str(self.dataframes["round_announce_match_start"][
            "tick"][0])

        # How many teamate kills
        df_player_sides = self.dataframes["parse_players"][["steamid", "starting_side"]]\
            .rename(columns={"steamid": "steamid_sides"})

        self.dataframes["player_death"] = pd.merge(self.dataframes["player_death"], df_player_sides,
                                                   how='left',
                                                   left_on=[
                                                       'attacker_steamid'],
                                                   right_on=['steamid_sides'])\
            .drop(columns=['steamid_sides'])\
            .rename(columns={"starting_side": "attacker_side"})
        self.dataframes["player_death"] = pd.merge(self.dataframes["player_death"], df_player_sides,
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
                .drop(columns=['attacker_steamid_teammates'])

        df_deaths = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
            .groupby('player_steamid')["player_steamid"]\
            .count().reset_index(name="death")

        df_assist = self.dataframes["player_death"]\
            .query("tick > "+tick_round_start)\
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
        print()

    def main(self):
        self.read_csv_to_pd()
        self.func_kda()


a = csgo_analyzer()
a.main()
