import os
import json
import unittest

from odl.prepare import get_ip_encoder


class TestEncoder(unittest.TestCase):
    def test_basic_encoder(self):

        # Should be different salts.
        assert get_ip_encoder()("test") != get_ip_encoder()("test")

        # same salt.
        assert get_ip_encoder("test")("test") == get_ip_encoder("test")("test")


if __name__ == '__main__':
    unittest.main()
