"""
Utility to grab the raw data for user_agents and blacklist.
"""

import os
import requests

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def download(url, file_path):
    resp = requests.get(url)
    with open(os.path.join(DIR_PATH, file_path), 'w') as file:
        file.write(resp.content)


def update():
    print("Downloading opawg user_agents and ipcat datacenters")
    download(
        'https://raw.githubusercontent.com/opawg/user-agents/master/src/user-agents.json',
        './odl/data/user-agents.json')

    download(
        'https://raw.githubusercontent.com/client9/ipcat/master/datacenters.csv',
        './odl/data/datacenters.csv')

    print("Updated opawg user_agents and ipcat datacenters")


if __name__ == '__main__':
    update()
