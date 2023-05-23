from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from demoparser import DemoParser
from app_analyze import CSGOAnalyzer
# ----- JRE -----
# https://www.java.com/en/download/manual.jsp
# windows offline 64
# instala
# JAVA_HOME = C:\Program Files\Java\jre-1.8

# ----- SPARK ----- 3.2.4
# https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
# extrair em C:\Spark
# SPARK_HOME = C:\Spark
# path adiciona %SPARK_HOME%\bin

# ----- WINUTILS ----- 3.2.2
# https://github.com/cdarlint/winutils/blob/master/hadoop-3.2.2/bin/winutils.exe
# ...
# Download
# cola em C:\Hadoop\bin
# HADOOP_HOME = c:/Hadoop
# path adiciona %HADOOP_HOME%\bin

# ----- PYSPARK -----
# pip install pyspark

# ----- TESTAR -----
# cmd como adminstrador> spark-shell


class CSGOParser:
    """
    Class responsible for parsing CSGO game match data.
    """
    LOCAL_CSV_PATH = "c:/projects/csgo_parser/csv"

    def __init__(self, demo_path):
        """
        Initializes the CSGOParser class.

        Args:
            demo_path (str): The path to the CSGO .dem file.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.parser = DemoParser(demo_path)
        self.match_date = self.get_date_from_demofile(demo_path)
        self.parse_players = self.parser.parse_players()
        self.parse_header = self.parser.parse_header()
        self.match_id = self.calculate_file_hash(demo_path)
        print()

    def get_date_from_demofile(self, file_path):
        """
        Gets the match date from the .dem file name.

        Args:
            file_path (str): The path to the CSGO .dem file.

        Returns:
            str: The match date.
        """
        return file_path[-17:-7]

    def calculate_file_hash(self, file_path):
        """
        Calculates the hash of the .dem file.

        Args:
            file_path (str): The path to the CSGO .dem file.

        Returns:
            str: The hash value of the file.
        """
        import zlib

        # Create a hash object
        hash_object = zlib.crc32(b'')

        # Read the file content
        with open(file_path, 'rb') as file:
            for block in iter(lambda: file.read(4096), b''):
                # Update the hash object with each block
                hash_object = zlib.crc32(block, hash_object)

        # Get the hash value as an 8-character hexadecimal representation
        file_hash = format(hash_object & 0xFFFFFFFF, '08x')

        return file_hash

    def save_to_csv(self, df, file_name):
        """
        Saves the dataframe data into a CSV file.

        Args:
            df (DataFrame): The dataframe to be saved.
            file_name (str): The name of the generated CSV file.
        """
        absolute_path = f"{self.LOCAL_CSV_PATH}/{file_name}.csv"
        df.write.csv(absolute_path, header=True, mode="overwrite")

    def rename_dataframe_columns(self, event, dataframe):
        """
        Renames the columns of the dataframe.

        Args:
            event (str): The event corresponding to the columns.
            dataframe (DataFrame): The dataframe to be renamed.

        Returns:
            DataFrame: The dataframe with renamed columns.
        """
        renamed_columns = [col(column).alias(
            f"{event.upper()}.{column.upper()}") for column in dataframe.columns]
        return dataframe.select(renamed_columns)

    def clean_remove_dataframe_columns(self, dataframe):
        """
        Removes columns with all missing or zero values from the dataframe.

        Args:
            dataframe (DataFrame): The dataframe to be cleaned.

        Returns:
            DataFrame: The cleaned dataframe.
        """
        # Remove columns with all missing values
        dataframe = dataframe.dropna(how='all')
        # Remove columns with all zero values
        dataframe = dataframe.select(
            [col for col in dataframe.columns if dataframe.where(col != 0).count() > 0])

        # Set the match ID in the dataframe
        dataframe = dataframe.withColumn("match_id", col(self.match_id))

        return dataframe

    def get_parsed(self, event):
        """
        Performs parsing of CSGO game events.

        Args:
            event (str): The name of the event to be parsed.

        Returns:
            DataFrame: The dataframe containing the parsed event data.
        """
        data_parsed = self.parser.parse_events(event)
        data_df = self.spark.createDataFrame(data_parsed)

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def get_parsed_players(self):
        """
        Performs parsing of CSGO game players.

        Returns:
            DataFrame: The dataframe containing the parsed player data.
        """
        data_parsed = self.parser.parse_players()
        data_df = self.spark.createDataFrame(data_parsed)

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def get_parsed_header(self):
        """
        Performs parsing of CSGO game header.

        Returns:
            DataFrame: The dataframe containing the parsed header data.
        """
        data_parsed = self.parse_header
        data_df = self.spark.createDataFrame(data_parsed)

        data_df = self.clean_remove_dataframe_columns(data_df)

        return data_df

    def load_all_events(self):
        """
        Loads all parsed events into CSV files.
        """
        self.save_to_csv(self.get_parsed_players(), "parse_players")
        self.save_to_csv(self.get_parsed_header(), "parse_header")

        all_events = self.list_all_events()
        for event in all_events:
            parsed_event = self.get_parsed(event)
            self.save_to_csv(parsed_event, event)

    def list_all_events(self):
        """
        Lists all unique events in the CSGO game.

        Returns:
            list: The list of unique events.
        """
        all_event_unique = []

        game_events = self.parser.parse_events("")
        for event in game_events:
            all_event_unique.append(event["event_name"])

        all_event_unique = list(set(all_event_unique))

        print(all_event_unique)
        return all_event_unique

    def main(self):
        """
        Main method for executing the steps of parsing and analyzing the CSGO game.
        """
        self.load_all_events()
        analyze = csgo_analyzer(self.match_id, self.match_date)
        analyze.main()


a = CSGOParser(
    "c:/projects/csgo_parser/data/003604372192294338675_1473557262.dem")
a.main()
print()
