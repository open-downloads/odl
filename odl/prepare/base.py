from __future__ import absolute_import

import csv
import json
from odl.avro import write_events

from .normalize import clean
from .encode import get_ip_encoder

from odl.exceptions import ODLError

__all__ = ["clean", "write", "get_ip_encoder"]


def write(rows, output_path, mappings=None, salt=None):
    """
    Given a set of data, we write a new odl.avro file to pull everything in.
    """
    # Clean these rows into something decent.
    resp = clean(rows, mappings=mappings, salt=salt)

    # Deal with writing resp.
    write_events(resp, output_path)


def run(input_path, output_path, format="csv", mappings=None, salt=None):
    """
    Given an input_path, try to parse the file and write it.
    """
    assert format in ["csv", "json", "json-line"]

    rows = []

    with open(input_path) as input_file:
        if format == "csv":
            reader = csv.DictReader(input_file)
            rows = list(reader)
        elif format == "json-line":
            for line in input_file:
                rows.append(json.loads(line))
        elif format == "json":
            rows = json.load(input_file)

    write(rows, output_path, mappings, salt)
