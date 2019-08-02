import os
import json
import unittest

from odl import prepare

DATA = """
[{
  "eventId": "f7d4357d-48b8-4608-9cc1-beac5f12bb6e",
  "city": "woodridge",
  "isByteRangeRequest": true,
  "byteRangeEnd": 1228799,
  "protocol": "https",
  "referrer": "",
  "country": "US",
  "region": "il",
  "longitude": -88.050341,
  "byteRangeStart": 1212416,
  "userAgent": "AppleCoreMedia/1.0.0.16F203 (iPod touch; U; CPU OS 12_3_1 like Mac OS X; en_us)",
  "latitude": 41.746975,
  "startedAt": "2019-07-22T22:59:59.987816349Z",
  "enclosureUrl": "https://podsights.com/example.mp3",
  "ipAddress": "1.200.300.4000",
  "method": "GET"
},
{
  "eventId": "529dfa25-cf36-49de-adb2-36826b1f05cd",
  "city": "nashville",
  "isByteRangeRequest": true,
  "byteRangeEnd": 1,
  "protocol": "https",
  "referrer": "",
  "country": "US",
  "region": "tn",
  "longitude": -86.781602,
  "byteRangeStart": 0,
  "userAgent": "AppleCoreMedia/1.0.0.16F203 (iPhone; U; CPU OS 12_3_1 like Mac OS X; en_us)",
  "latitude": 36.162664,
  "startedAt": "2019-07-22T23:00:00.160001647Z",
  "enclosureUrl": "https://podsights.com/example.mp3",
  "ipAddress": "2600:2600:2600:1000:1000:1000:1000:1000",
  "method": "GET"
}]"""


class TestNormalize(unittest.TestCase):
    def test_basic_normalize(self):
        data = json.loads(DATA)
        resp = prepare.clean(data, mappings={"startedAt": "timestamp"})

        assert resp[0]["http_method"] == "GET"
        assert "ip" not in resp[0]

        assert resp[1]["http_method"] == "GET"
        assert "ip" not in resp[1]


if __name__ == '__main__':
    unittest.main()
