import os
import re
import json
import time

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

__all__ = ["get_app"]


def get_useragents():
    with open(os.path.join(DIR_PATH, './data/user-agents.json'),
              'rb') as jsonfile:
        return json.load(jsonfile)


class Players(object):
    """
    Uses opawg's excellent user_agents list to remove downloads from known
    bots and build downloads by user_agent.

    https://github.com/opawg/user-agents
    """
    _db = None

    def get(self, user_agent):
        if self._db is None:
            self._db = []

            # Create a db of (re, {data})
            for row in get_useragents():

                for ua in row['user_agents']:
                    self._db.append((re.compile(ua), {
                        "app": row.get("app", "Unknown"),
                        "device": row.get("device", "Unknown"),
                        "os": row.get("os", "Unknown"),
                        "bot": row.get("bot", False),
                    }))

                # We assume anything without a user_agent is a bot.
                self._db.append((re.compile('^(|\s+)$'), {
                    "app": "Unknown",
                    "device": "Unknown",
                    "os": "Unknown",
                    "bot": True
                }))

        # Searh the db for
        for ua_re, data in self._db:
            if ua_re.search(user_agent):
                resp = data.copy()
                resp["user_agent"] = user_agent
                return resp

        # by default we don't want to trash unknown agents.
        return {
            "user_agent": user_agent,
            "app": "Unknown",
            "device": "Unknown",
            "os": "Unknown",
            "bot": False
        }


players = Players()


def get_player(user_agent):
    return players.get(user_agent)
