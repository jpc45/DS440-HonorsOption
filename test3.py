#!/usr/bin/env python
# Licensed to Elasticsearch B.V under one or more agreements.
# Elasticsearch B.V licenses this file to you under the Apache 2.0 License.
# See the LICENSE file in the project root for more information

"""Script that downloads a public dataset and streams it to an Elasticsearch cluster"""
#curl -X DELETE 'http://localhost:9200/nba-test'

import csv
from os.path import abspath, join, dirname, exists
import tqdm
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


DATASET_PATH = 'NBAstats.csv'
CHUNK_SIZE = 16384


def download_dataset():
    """Downloads the public dataset if not locally downlaoded
    and returns the number of rows are in the .csv file.
    """
    with open(DATASET_PATH) as f:
        return sum([1 for _ in f]) - 1


def create_index(client):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index="nba-test",
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
#                    "Name" : {"type": "text"},
                    "Team" : {"type": "keyword"},
                    "Position" : {"type": "keyword"},
                    "Age" : {"type": "keyword"},
                    "GP" : {"type": "keyword"},
                    "MPG" : {"type": "keyword"},
                    "Minutes Percentage" : {"type": "keyword"},
                    "Usage Rate" : {"type": "keyword"},
                    "Turnover Rate" : {"type": "keyword"},
                    "FTA" : {"type": "keyword"},
                    "FT%" : {"type": "keyword"},
                    "2PA" : {"type": "keyword"},
                    "2P%" : {"type": "keyword"},
                    "3PA" : {"type": "keyword"},
                    "3P%" : {"type": "keyword"},
                    "Effective Shooting Percentage" : {"type": "keyword"},
                    "True Shooting Percentage" : {"type": "keyword"},
                    "Points per game" : {"type": "keyword"},
                    "Rebounds per game" : {"type": "keyword"},
                    "Total Rebound Percentage" : {"type": "keyword"},
                    "Assists per game" : {"type": "keyword"},
                    "Assist Percentage" : {"type": "keyword"},
                    "Steals per game" : {"type": "keyword"},
                    "Blocks per game" : {"type": "keyword"},
                    "Turnovers per game." : {"type": "keyword"},
                    "Versatility index" : {"type": "keyword"},
                    "Offensive Rating" : {"type": "keyword"},
                    "Defensive Rating" : {"type": "keyword"},
                }
            },
        },
        ignore=400,
    )


def generate_actions():
    """Reads the file through csv.DictReader() and for each row
    yields a single document. This function is passed into the bulk()
    helper to create many documents in sequence.
    """
    with open(DATASET_PATH, mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            doc = {
#                "Name" : row["Player"],
                "Team" : row["TEAM"],
                "Position" : row["POS"],
                "Age" : row["AGE"],
                "GP" : row["GP"],
                "MPG" : row["MPG"],
                "Minutes Percentage" : row["Minutes Percentage"],
                "Usage Rate" : row["Usage Rate"],
                "Turnover Rate" : row["Turnover Rate"],
                "FTA" : row["FTA"],
                "FT%" : row["FT%"],
                "2PA" : row["2PA"],
                "2P%" : row["2P%"],
                "3PA" : row["3PA"],
                "3P%" : row["3P%"],
                "Effective Shooting Percentage" : row["Effective Shooting Percentage"],
                "True Shooting Percentage" : row["True Shooting Percentage"],
                "Points per game" : row["Points per game"],
                "Rebounds per game" : row["Rebounds per game"],
                "Total Rebound Percentage" : row["Total Rebound Percentage"],
                "Assists per game" : row["Assists per game"],
                "Assist Percentage" : row["Assist Percentage"],
                "Steals per game" : row["Steals per game"],
                "Blocks per game" : row["Blocks per game"],
                "Turnovers per game." : row["Turnovers per game."],
                "Versatility index" : row["Versatility index"],
                "Offensive Rating" : row["Offensive Rating"],
                "Defensive Rating" : row["Defensive Rating"],
            }
            yield doc


def main():
    print("Loading dataset...")
    number_of_docs = download_dataset()

    client = Elasticsearch(
        [{'host':'localhost','port':9200}]
    )
    print("Creating an index...")
    create_index(client)

    print("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(
        client=client, index="nba-test", actions=generate_actions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, number_of_docs))


if __name__ == "__main__":
    main()


#curl -X GET http://localhost:9200/nba-test/_search?q=Mia