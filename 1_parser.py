from demoparser import DemoParser
import pandas as pd
import boto3
import os


class csgo_parser():
    # ACCESS_KEY_ID = ""
    # SECRET_ACCESS_KEY = ""
    # BUCKET = ""
    # Key = ""
    # ACL = ""
    LOCAL_CSV_PATH = "c:\csgo_app\csv"

    def __init__(self, demo_path):
        self.parser = DemoParser(demo_path)
        self.parse_players = self.parser.parse_players()
        self.parse_header = self.parser.parse_header()
        self.match_id = self.calculate_file_hash(demo_path)
        print()

    def calculate_file_hash(self, file_path):
        import zlib

        # Criar um objeto de hash
        hash_object = zlib.crc32(b'')

        # Ler o conteúdo do arquivo em blocos
        with open(file_path, 'rb') as file:
            for block in iter(lambda: file.read(4096), b''):
                # Atualizar o objeto de hash com cada bloco de dados
                hash_object = zlib.crc32(block, hash_object)

        # Obter o valor de hash como uma representação hexadecimal de até 8 caracteres
        file_hash = format(hash_object & 0xFFFFFFFF, '08x')

        return file_hash

    # def configure_s3_connection(self):
    #     s3 = boto3.client(
    #         's3',
    #         aws_access_key_id='SEU_ACCESS_KEY_ID',
    #         aws_secret_access_key='SEU_SECRET_ACCESS_KEY'
    #     )

    #     return s3

    # def upload_to_s3(self, data):
    #     s3_clinet = self.configure_s3_connection()

    #     s3_clinet.put_object(Body=data,
    #                          Bucket='seu_bucket',
    #                          Key="path",
    #                          #  ACL='public-read',
    #                          ContentType='text/csv',
    #                          #  ContentEncoding='utf-8',
    #                          #  Metadata=""
    #                          )

# !!!!Adicionar retry nottations
# !!!! remove the X variable after the retry notations
    def save_to_csv(self, df, file_name):
        """
            Save the dataframe data into a csv on the path set in LOCAL_CSV_PATH variable
            return: boolean
        """
        absolute_path = self.LOCAL_CSV_PATH+"/"+file_name+".csv"
        df.to_csv(absolute_path, index=False)

    def rename_dataframe_columns(self, event, dataframe):
        dataframe = dataframe.rename(
            columns=lambda x: event.upper()+"."+x.upper())
        return dataframe

    def clean_remove_dataframe_columns(self, dataframe):
        # Remove colunas com todos os valores ausentes
        dataframe = dataframe.dropna(axis=1, how='all')
        # Remove colunas com todos os valores 0
        dataframe = dataframe.loc[:, (dataframe != 0).any(axis=0)]

        # Setting the match ID to the data_frame
        dataframe['match_id'] = self.match_id
        return dataframe

    def get_parsed(self, event):
        data_parsed = self.parser.parse_events(event)

        data_df = pd.DataFrame(data_parsed)

        # data_df = self.rename_dataframe_columns(event, data_df)
        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def get_parsed_players(self):
        data_parsed = self.parser.parse_players()

        data_df = pd.DataFrame(data_parsed)

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def load_events(self):
        # self.player_death = self.get_parsed("hltv_status")
        # self.player_death = self.get_parsed("hltv_message")
        # self.player_death = self.get_parsed("hltv_fixed")
        # self.player_death = self.get_parsed("hltv_chase")
        # self.player_death = self.get_parsed("cs_pre_restart")
        # self.player_death = self.get_parsed("round_prestart")
        # self.player_death = self.get_parsed("round_start")
        # self.player_death = self.get_parsed("round_announce_warmup")
        # self.player_death = self.get_parsed("round_poststart")
        # self.player_death = self.get_parsed("cs_round_start_beep")
        # self.player_death = self.get_parsed("cs_round_final_beep")
        # self.player_death = self.get_parsed("round_freeze_end")
        # self.player_death = self.get_parsed("buytime_ended")
        # self.player_death = self.get_parsed("begin_new_match")
        # self.player_death = self.get_parsed("round_announce_match_start")
        # self.player_death = self.get_parsed("cs_win_panel_round")
        # self.player_death = self.get_parsed("round_end")
        # self.player_death = self.get_parsed("round_time_warning")
        # self.player_death = self.get_parsed("round_officially_ended")
        # self.player_death = self.get_parsed("inferno_startburn")
        # self.player_death = self.get_parsed("inferno_expire")
        # self.player_death = self.get_parsed("other_death")
        # self.player_death = self.get_parsed("round_announce_last_round_half")
        # self.player_death = self.get_parsed("announce_phase_end")
        # self.player_death = self.get_parsed("round_announce_match_point")
        # self.player_death = self.get_parsed("cs_win_panel_match")

        self.player_death = self.get_parsed("player_death")
        self.match_end_conditions = self.get_parsed("match_end_conditions")
        # self.player_hurt = self.get_parsed("player_hurt")  # para ADR

        print("aa")

    def load_all_events(self):
        self.save_to_csv(self.get_parsed_players(), "parse_players")

        all_events = self.list_all_events()
        for x in all_events:
            event = self.get_parsed(x)
            # self.upload_to_s3(event)
            self.save_to_csv(event, x)

    def list_all_events(self):
        all_event_unique = []

        game_events = self.parser.parse_events("")
        for event in game_events:
            all_event_unique.append(event["event_name"])

        all_event_unique = list(dict.fromkeys(all_event_unique))

        print(all_event_unique)
        return all_event_unique

    # def read_csv_to_pd(self):
    #     # Removing all dataframes from memory
    #     lst = [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
    #     del lst

    #     csv_files = os.listdir(self.LOCAL_CSV_PATH)
    #     self.dataframes = {}

    #     for file in csv_files:
    #         if file.endswith('.csv'):
    #             # Remove a extensão .csv do nome do arquivo
    #             nome_dataframe = file[:-4]
    #             caminho_arquivo = os.path.join(self.LOCAL_CSV_PATH, file)
    #             self.dataframes[nome_dataframe] = pd.read_csv(caminho_arquivo)

    # def func_kda(self):
    #     self.read_csv_to_pd()

    def main(self):
        self.load_events()

        # Kill
        for x in range(len(self.parse_players)):
            print(self.parse_players[x])
            player_name = self.parse_players[x]["name"]

            self.parse_players[x].update(
                {"kills": len(self.player_death
                              .query("attacker_name == '{}'"
                                     .format(player_name)))})

        # Death
        for x in range(len(self.parse_players)):
            print(self.parse_players[x])
            player_name = self.parse_players[x]["name"]

            self.parse_players[x].update(
                {"death": len(self.player_death
                              .query("player_name == '{}'"
                                     .format(player_name)))})

        # Assist
        for x in range(len(self.parse_players)):
            print(self.parse_players[x])
            user_id = self.parse_players[x]["user_id"]

            self.parse_players[x].update(
                {"assist": len(self.player_death
                               .query("assister == '{}'"
                                      .format(user_id)))})

    #  map_name [:find("\")]        "_" signon_length  protoplayback_tickscol  playback_frames
# id de_overpass                     _  566301         189267                  94539           167841343781153874
# id de_overpass_56630118926794539167841343781153874
a = csgo_parser("c:/csgo_app/data/003604372192294338675_1473557262.dem")
# a.list_all_events()
a.load_all_events()
# a.load_events()
# a.main()
print('End!')
