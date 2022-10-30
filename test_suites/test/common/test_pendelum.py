import unittest
from datetime import date

import pendulum

from buffett.common.pendelum import Date, DateSpan, DateTime, TimeSpan, Duration


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
        dt2 = DateTime(2022, 1, 4, 9, 30)
        dt3 = DateTime(2022, 1, 4)
        assert date1 < dt2
        assert date1 <= dt2
        assert dt2 > date1
        assert dt2 >= date1
        assert date1 == dt3
        assert date1 != dt2

    def test_datespan(self):
        span = DateSpan(Date(2022, 1, 4), Date(2022, 1, 5))
        assert span._start.year == 2022
        assert span._start.month == 1
        assert span._start.day == 4
        assert span._end.year == 2022
        assert span._end.month == 1
        assert span._end.day == 5

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

    def test_add_sub(self):
        dt1 = DateTime(2022, 1, 1)
        du = Duration(days=1)
        # test add
        dt2 = dt1 + du
        dt3 = DateTime(2022, 1, 2)
        assert type(dt2) == DateTime
        assert dt2 == dt3
        dt2 = dt1.add(days=1)
        assert type(dt2) == DateTime
        assert dt2 == dt3
        # test sub
        # test add
        dt2 = dt1 - du
        dt3 = DateTime(2021, 12, 31)
        assert type(dt2) == DateTime
        assert dt2 == dt3
        dt2 = dt1.subtract(days=1)
        assert type(dt2) == DateTime
        assert dt2 == dt3

    def test_now(self):
        now1 = DateTime.now()
        now2 = pendulum.DateTime.now()
        assert now1.year == now2.year
        assert now1.month == now2.month
