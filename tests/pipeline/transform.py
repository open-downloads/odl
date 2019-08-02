import os
import json
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from odl import avro
from odl.pipeline import transforms

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def print_item(i):
    print i
    return i


class TestODLWindow(unittest.TestCase):
    def test_window(self):

        pipeline_options = {
            'runner': 'DirectRunner',
            'project': 'adaptive-growth'
        }

        options = PipelineOptions.from_dictionary(pipeline_options)

        with TestPipeline(options=options) as p:

            # load up the fixture.
            events = (p | beam.Create(
                list(
                    avro.read(
                        os.path.join(DIR_PATH,
                                     '../../fixtures/demo.odl.avro')))))

            downloads = (events | transforms.ODLDownloads(3600))

            p_r = (downloads | beam.Map(print_item))

            c_h = (downloads | transforms.CountByHour()
                   | 'hour' >> beam.Map(print_item))
            c_a = (downloads | transforms.CountByApp()
                   | 'a' >> beam.Map(print_item))
            c_e = (downloads | transforms.CountByEpisode()
                   | 'e' >> beam.Map(print_item))

            d_g = (downloads | transforms.CountDownloads()
                   | 'c' >> beam.Map(print_item))


if __name__ == '__main__':
    unittest.main()
