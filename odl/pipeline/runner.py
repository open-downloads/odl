import os
import shutil
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from odl.pipeline import transforms, outputs

base_options = {"save_main_session": False, "runner": "DirectRunner"}


def _to_absolute_path(path):
    if path.startswith('/'):
        return path

    if '://' in path:
        return path

    return os.path.join(os.getcwd(), path)


def _move(output_path, file_name):
    name, suffix = file_name.split('.')

    shutil.move(
        os.path.join(output_path, '{}-00000-of-00001.{}'.format(name, suffix)),
        os.path.join(output_path, file_name))


def run(path, output_path, options=None, offset=None):
    """
    Run the oDL pipeline.

    path: Where we are getting the download data.
    offset: Offset in seconds from UTC for the attribution window.
    """

    print("Running the oDL pipeline from {} to {}".format(path, output_path))

    # if it's a relative path, we need to make it absolue.
    path = _to_absolute_path(path)
    output_path = _to_absolute_path(output_path)

    # create the actual pipeline
    pipeline_options = base_options.copy()
    if options:
        pipeline_options.update(options)

    options = PipelineOptions.from_dictionary(pipeline_options)
    pipeline = beam.Pipeline(options=options)

    # We are going to
    events = (pipeline | beam.io.avroio.ReadFromAvro(path))

    # Does the bulk of the work.
    downloads = (events | transforms.ODLDownloads(window_offset=offset))

    # Get hourly Downloads.
    hourly = (downloads | 'CountByHour' >> transforms.CountByHour())

    # Downloads by Episode
    episodes = (downloads | 'CountByEpisode' >> transforms.CountByEpisode())

    # Downloads by App.
    apps = (downloads | 'CountByApp' >> transforms.CountByApp())

    # Total number of downloads.
    count = (downloads | 'CountDownloads' >> transforms.CountDownloads())

    # Write everything out. We are going to use CSV formats for hourly,
    # episodes, etc and avro for the downloads again.
    hourly_out = (hourly
                  | 'WriteHourly' >> outputs.WriteCSV(output_path, 'hourly'))

    # Write episodes
    episodes_out = (
        episodes
        | 'WriteEpisodes' >> outputs.WriteCSV(output_path, 'episodes'))

    # Write apps
    apps_out = (apps | 'WriteApps' >> outputs.WriteCSV(output_path, 'apps'))

    # Write count
    count_out = (count | 'WriteCount' >> outputs.Write(output_path, 'count'))

    downloads_out = (downloads
                     | 'WriteDownloads' >> outputs.WriteDownloads(output_path))

    # Actually start this thing and wait till it's done.
    resp = pipeline.run()
    resp.wait_until_finish()

    if output_path.startswith('/'):

        with open(os.path.join(output_path,
                               'count-00000-of-00001.txt')) as file:
            print("\noDL run complete.\nDownloads: {}".format(
                file.read().strip()))

        for file_name in [
                'count.txt', 'hourly.csv', 'episodes.csv', 'apps.csv'
        ]:
            _move(output_path, file_name)
            print("\n{}".format(os.path.join(output_path, file_name)))
