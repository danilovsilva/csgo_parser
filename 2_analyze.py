from demoparser import DemoParser
import pandas as pd
import os


class csgo_analyzer():
    LOCAL_CSV_PATH = "c:\csgo_app\csv"

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
        # Calculating the Kill number for each player
        df_kda = self.dataframes["parse_players"][['steamid', 'name']]
        df_kills = self.dataframes["player_death"]\
            .query("attacker_steamid != 0")\
            .groupby('attacker_steamid')["attacker_steamid"]\
            .count().reset_index(name="kills")
        df_deaths = self.dataframes["player_death"]\
            .groupby('player_steamid')["player_steamid"]\
            .count().reset_index(name="death")

        df_kda = pd.merge(df_kda, df_kills, how='left', left_on=[
                          'steamid'], right_on=['attacker_steamid'])\
            .drop(columns=['attacker_steamid'])
        df_kda = pd.merge(df_kda, df_deaths, how='left', left_on=[
                          'steamid'], right_on=['player_steamid'])\
            .drop(columns=['player_steamid'])
        print()

    def main(self):
        self.read_csv_to_pd()
        self.func_kda()


a = csgo_analyzer()
a.main()
