from demoparser import DemoParser
import pandas as pd
# import boto3
import os
import passwd

class csgo_parser():
    LOCAL_CSV_PATH = "c:\csgo_app\csv"

    def __init__(self, demo_path):
        self.parser = DemoParser(demo_path)
        self.demo_date = self.get_date_from_demofile(demo_path)
        self.parse_players = self.parser.parse_players()
        self.parse_header = self.parser.parse_header()
        self.match_id = self.calculate_file_hash(demo_path)

    def get_date_from_demofile(self,file_path):
        return file_path[-17:-7]        
        
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

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def get_parsed_players(self):
        data_parsed = self.parser.parse_players()

        data_df = pd.DataFrame(data_parsed)

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def load_all_events(self):
        self.save_to_csv(self.get_parsed_players(), "parse_players")

        all_events = self.list_all_events()
        for x in all_events:
            event = self.get_parsed(x)
            self.save_to_csv(event, x)

    def list_all_events(self):
        all_event_unique = []

        game_events = self.parser.parse_events("")
        for event in game_events:
            all_event_unique.append(event["event_name"])

        all_event_unique = list(dict.fromkeys(all_event_unique))

        print(all_event_unique)
        return all_event_unique

    def main(self):
        self.load_all_events()

a = csgo_parser("c:/csgo_app/data/003604372192294338675_1473557262.dem")
a.main()

