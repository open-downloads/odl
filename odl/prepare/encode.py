import uuid

import hashlib
from odl.exceptions import ODLError


def get_ip_encoder(salt=None):
    """
    A one way encoder for dealing with ips at rest.

    by default, oDL will create a one time salt to encode the IP on a per file
    basis. If you are using this across files or over time, then you need to
    supply your own salt.
    """
    salt = salt and salt or uuid.uuid4().hex

    if isinstance(salt, unicode):
        salt = salt.encode('utf-8')

    def encode(ip):
        if not ip:
            raise ODLException('IP is required for encoding.')

        return hashlib.md5('|'.join([ip.encode('utf-8'), salt])).hexdigest()

    return encode
