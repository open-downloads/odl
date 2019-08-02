import os
import json
import unittest

from odl import blacklist


class TestBlacklist(unittest.TestCase):
    def test_basic_blacklist(self):

        # it doesn't match?
        assert not blacklist.is_blacklisted("test")

        assert blacklist.is_blacklisted("3.120.0.0")
        assert blacklist.is_blacklisted("3.120.0.1")


if __name__ == '__main__':
    unittest.main()
