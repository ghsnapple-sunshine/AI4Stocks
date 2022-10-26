import unittest
from datetime import date

from ai4stocks.common.pendelum import Date, DateSpan, DateTime, TimeSpan


class PendelumTest(unittest.TestCase):
    def test_date_compare(self):
        date1 = Date(2022, 1, 4)
        date2 = Date(2022, 1, 5)
        date3 = Date(2022, 1, 4)
        assert date1 < date2
        assert date1 <= date2
        assert date2 > date1
        assert date2 >= date1
        assert date1 == date3
        assert date1 != date2

    def test_datetime_compare(self):
        datetime1 = DateTime(2022, 1, 4)
        datetime2 = DateTime(2022, 1, 5)
        datetime3 = DateTime(2022, 1, 4)
        assert datetime1 < datetime2
        assert datetime1 <= datetime2
        assert datetime2 > datetime1
        assert datetime2 >= datetime1
        assert datetime1 == datetime3
        assert datetime1 != datetime2

    def test_date_datetime_comapre(self):
        date1 = Date(2022, 1, 4)
        datetime2 = DateTime(2022, 1, 4, 9, 30)
        datetime3 = DateTime(2022, 1, 4)
        assert date1 < datetime2
        assert date1 <= datetime2
        assert datetime2 > date1
        assert datetime2 >= date1
        assert date1 == datetime3
        assert date1 != datetime2

    def test_datespan(self):
        span = DateSpan(Date(2022, 1, 4), Date(2022, 1, 5))
        assert span.start.year == 2022
        assert span.start.month == 1
        assert span.start.day == 4
        assert span.end.year == 2022
        assert span.end.month == 1
        assert span.end.day == 5

    def test_timespan(self):
        span = TimeSpan(DateTime(2022, 1, 4), DateTime(2022, 1, 5))
        assert span.start.year == 2022
        assert span.start.month == 1
        assert span.start.day == 4
        assert span.end.year == 2022
        assert span.end.month == 1
        assert span.end.day == 5

    def test_hash(self):
        date1 = Date(2022, 1, 1)
        _hash1 = hash(date1)
        assert _hash1 != -1
        date2 = date(2022, 1, 1)
        _hash2 = hash(date2)
        assert _hash1 == _hash2
