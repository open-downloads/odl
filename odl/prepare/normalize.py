import re
import json
import arrow

import fastavro

from fastavro import writer, parse_schema

from odl import blacklist
from odl.exceptions import ODLError
from odl.prepare.encode import get_ip_encoder


def clean_string(value):
    return value.strip()


def clean_timestamp(value):
    """
    Arrow is not fast, so we should move to something more restrictive at
    somepoint.
    """

    for fmt in [
            'YYYY-MM-DDTHH:mm:ss.S[Z]', 'YYYY-MM-DDTHH:mm:ss.SZZ',
            'YYYY-MM-DDTHH:mm:ss.SZ', 'YYYY-MM-DDTHH:mm:ss.S'
    ]:
        try:
            a = arrow.get(value)
            return a.to('UTC').isoformat()
        except:
            pass

    raise ValueError("unable to parse timestamp {}".format(value))


def clean_int(value):
    if isinstance(value, (int, float)):
        return int(value)

    if isinstance(value, (str, unicode)):
        return int(value.strip())

    raise ValueError("Invalid type {}".format(str(type('a'))))


fields = {
    "encoded_ip": {
        "required": True,
        "clean": lambda e: e
    },
    "user_agent": {
        "required": True,
        "clean": clean_string
    },
    "http_method": {
        "required": True,
        "clean": clean_string
    },
    "timestamp": {
        "required": True,
        "clean": clean_timestamp
    },
    "episode_id": {
        "required": True,
        "clean": clean_string
    },
    "byte_range_start": {
        "required": True,
        "clean": clean_int
    },
    "byte_range_end": {
        "required": True,
        "clean": clean_int
    },
}

snake_case_re = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')


def to_snake_case(value):
    return snake_case_re.sub(r'_\1', value).lower()


# mappings to make everything a bit easier.
default_mappings = {
    'ipaddress': 'ip',
    'ip_address': 'ip',
    'useragent': 'user_agent',
    'encodedip': 'encoded_ip',
    'enclosureurl': 'episode_id',
    'enclosure_url': 'episode_id',
    "method": "http_method"
}


def to_mapping(mappings):
    """
    Allow user generated mappings.
    """
    map = default_mappings.copy()

    if mappings:
        for key, value in mappings.items():
            map[to_snake_case(key)] = value

    return map


def to_key(value, mapping):
    """
    Normalize the key to something we want.
    """
    key = to_snake_case(value)

    if key in mapping:
        return mapping[key]

    return key


def verify(data, original):
    """
    Verify each row.
    """
    error_str = "Row: '{}' Original: '{}' ".format(
        json.dumps(data), json.dumps(original))

    if 'encoded_ip' not in data:
        raise ODLError(
            "ODL requires an `encoded_ip` or `ip` attribute. {}".format(
                error_str))

    for field, meta in fields.items():
        value = data.get(field)

        if meta['required'] and not value:
            if field not in data:
                raise ODLError("ODL requires the attribute {} {}".format(
                    field, error_str))

        if value:
            try:
                value = meta['clean'](value)
            except Exception, e:
                raise ODLError('{} {}'.format(str(e), error_str))

            data[field] = value

    return data


def clean(rows, mappings=None, salt=None):
    """
    given a row of data it will try and pull out the data that we need.

    it's fairly forgiving on field names.
    """

    mapping = to_mapping(mappings)

    # Just incase we need it.
    ip_encoder = get_ip_encoder(salt=salt)

    resp = []

    for row in rows:
        data = {}

        for raw_key, value in row.items():
            key = to_key(raw_key, mapping)

            if key in fields.keys():
                data[key] = value

            if key == 'ip':
                data[key] = value

        # We want to kill bad IPs here, otherwise we lose this ability
        # after we encode the ip.
        if 'ip' in data:
            if blacklist.is_blacklisted(data['ip']):
                continue

            data['encoded_ip'] = ip_encoder(data['ip'])
            del data["ip"]

        # make sure that we have everything we need.
        data = verify(data, row)

        resp.append(data)

    return resp
