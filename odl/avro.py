"""
oDL prepares everything to a single avro schema before running the DataFlow
pipelines.
"""
from fastavro import reader, writer, parse_schema

# Used for the raw server events.
events_schema = {
    'doc':
    'oDL Event Schema',
    'name':
    'oDL Events',
    'namespace':
    'odl',
    'type':
    'record',
    'fields': [{
        'name': 'encoded_ip',
        'type': 'string'
    }, {
        'name': 'user_agent',
        'type': 'string'
    }, {
        'name': 'http_method',
        'type': 'string'
    }, {
        'name': 'timestamp',
        'type': 'string'
    }, {
        'name': 'episode_id',
        'type': 'string'
    }, {
        'name': 'byte_range_start',
        'type': ["int", "null"]
    }, {
        'name': 'byte_range_end',
        'type': ["int", "null"]
    }],
}
events_parsed = parse_schema(events_schema)

downloads_schema = parse_schema({
    'doc':
    'oDL Download Schema',
    'name':
    'oDL Downloads',
    'namespace':
    'odl',
    'type':
    'record',
    'fields': [{
        'name': 'id',
        'type': 'string'
    }, {
        'name': 'encoded_ip',
        'type': 'string'
    }, {
        'name': 'user_agent',
        'type': 'string'
    }, {
        'name': 'timestamp',
        'type': 'string'
    }, {
        'name': 'episode_id',
        'type': 'string'
    }, {
        'name': 'app',
        'type': 'string'
    }],
})

downloads_parsed = parse_schema(downloads_schema)


def write_events(rows, file_path):
    """
    write the input files.
    """
    with open(file_path, 'wb') as file:
        writer(file, events_parsed, rows)


def read(file_path):
    with open(file_path, 'rb') as file:
        avro_reader = reader(file)
        for record in avro_reader:
            yield record
