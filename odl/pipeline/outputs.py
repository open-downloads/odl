import csv
import os
import json
from cStringIO import StringIO
from avro.schema import parse as Parse

import apache_beam as beam
import apache_beam.transforms.window as window

from odl.avro import downloads_schema


def to_csv(item):
    """
    Uses the csv lib to excape everything correctly.
    """
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(list(item))
    return buf.getvalue().rstrip(csv.excel.lineterminator)


class WriteCSV(beam.PTransform):
    """
    Writes a CSV.
    """

    def __init__(self, file_path, name, label=None):
        super(WriteCSV, self).__init__(label=label)
        self.file_path = file_path
        self.name = name

    def expand(self, items):
        path = os.path.join(self.file_path, self.name)
        return (items | 'ToCSV' >> beam.Map(to_csv)
                | 'WriteCSV' >> beam.io.WriteToText(
                    path, file_name_suffix='.csv', num_shards=1))


class Write(beam.PTransform):
    """
    Just write.
    """

    def __init__(self, file_path, name, label=None):
        super(Write, self).__init__(label=label)
        self.file_path = file_path
        self.name = name

    def expand(self, items):
        path = os.path.join(self.file_path, self.name)
        return (items
                | 'WriteCSV' >> beam.io.WriteToText(
                    path, file_name_suffix='.txt', num_shards=1))


class WriteDownloads(beam.PTransform):
    def __init__(self, file_path, label=None):
        super(WriteDownloads, self).__init__(label=label)
        self.file_path = file_path

    def expand(self, downloads):
        path = os.path.join(self.file_path, 'downloads')

        return (
            downloads
            |
            'DownloadsGlobalWindow' >> beam.WindowInto(window.GlobalWindows())
            | beam.io.avroio.WriteToAvro(
                path,
                Parse(json.dumps(downloads_schema)),
                use_fastavro=True,
                file_name_suffix='.avro'))
