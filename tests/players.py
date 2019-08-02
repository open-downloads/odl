import os
import csv
import json
import unittest

from odl import players

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestPlayers(unittest.TestCase):
    def test_basic_players(self):

        with open(os.path.join(DIR_PATH, '../fixtures/user_agents.csv'),
                  'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                ua, percent = row
                player = players.get_player(ua)

                if not player['bot'] and player["app"] == "Unknown" and player["os"] == "Unknown":
                    print ua


if __name__ == '__main__':
    unittest.main()
