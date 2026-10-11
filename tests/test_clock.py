from __future__ import annotations
import unittest
from chronosqueue.clock import format_timestamp_us, parse_timestamp_us

class ClockCase(unittest.TestCase):
    def test_microsecond_roundtrip_without_float_loss(self):
        text="2026-10-11T15:42:13.123457+03:00"
        us=parse_timestamp_us(text)
        self.assertEqual("2026-10-11T12:42:13.123457Z",format_timestamp_us(us))
        self.assertEqual(us,parse_timestamp_us(format_timestamp_us(us)))
    def test_naive_timestamp_rejected(self):
        with self.assertRaises(ValueError): parse_timestamp_us("2026-10-11T12:00:00")

if __name__=='__main__': unittest.main()
