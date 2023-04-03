from demoparser import DemoParser
import pandas as pd


class csgo_parser():

    def __init__(self, demo_path):
        self.parser = DemoParser(demo_path)
        self.parse_players = self.parser.parse_players()
        for x in range(len(self.parse_players)):
            [self.parse_players[x].pop(k) for k in [
                'rank_id', 'rank_name', 'entity_id', 'comp_wins', 'crosshair_code']]

    def get_parsed(self, event):
        data_parsed = self.parser.parse_events(event)

        data_df = pd.DataFrame(data_parsed)
        # data_df = data_df[["attacker_name", ]]
        print(data_df)
        return data_df

    # def get_players_from_demo(self):
    #     players_in_match = {}
    #     for x in self.parse_players:

    #         players_in_match.update({
    #             x["name"]: {
    #                 "player_id": x["user_id"],
    #                 "player_steamid": x["steamid"],
    #                 "player_starting_side": x["starting_side"]
    #             }
    #         })

        # return players_in_match

    def load_events(self):
        self.player_death = self.get_parsed("player_death")
        # self.match_end_conditions = self.get_parsed("match_end_conditions")
        # self.player_hurt = self.get_parsed("player_hurt")  # para ADR

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


a = csgo_parser("c:/csgo_app/data/003604372192294338675_1473557262.dem")
a.main()
print(a.parse_players)
print('a')
