from buffett.adapter import pendulum
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import date
from buffett.common.pendelum import Date, DateSpan, DateTime, Duration
from test import Tester


class PendulumTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    @staticmethod
    def test_date_compare():
        date1 = Date(2022, 1, 4)
        date2 = Date(2022, 1, 5)
        date3 = Date(2022, 1, 4)
        assert date1 < date2
        assert date1 <= date2
        assert date2 > date1
        assert date2 >= date1
        assert date1 == date3
        assert date1 != date2

    @staticmethod
    def test_datetime_compare():
        datetime1 = DateTime(2022, 1, 4)
        datetime2 = DateTime(2022, 1, 5)
        datetime3 = DateTime(2022, 1, 4)
        assert datetime1 < datetime2
        assert datetime1 <= datetime2
        assert datetime2 > datetime1
        assert datetime2 >= datetime1
        assert datetime1 == datetime3
        assert datetime1 != datetime2

    @staticmethod
    def test_date_datetime_compare():
        date1 = Date(2022, 1, 4)
        dt2 = DateTime(2022, 1, 4, 9, 30)
        dt3 = DateTime(2022, 1, 4)
        assert date1 < dt2
        assert date1 <= dt2
        assert dt2 > date1
        assert dt2 >= date1
        assert date1 == dt3
        assert date1 != dt2

    @staticmethod
    def test_datespan():
        span = DateSpan(Date(2022, 1, 4), Date(2022, 1, 5))
        assert span._start.year == 2022
        assert span._start.month == 1
        assert span._start.day == 4
        assert span._end.year == 2022
        assert span._end.month == 1
        assert span._end.day == 5

    @staticmethod
    def test_timespan():
        span = DateSpan(DateTime(2022, 1, 4, 9, 30), DateTime(2022, 1, 5))
        assert span.start.year == 2022
        assert span.start.month == 1
        assert span.start.day == 4
        assert span.start.hour == 9
        assert span.start.minute == 30
        assert span.end.year == 2022
        assert span.end.month == 1
        assert span.end.day == 5

    @staticmethod
    def test_hash():
        date1 = Date(2022, 1, 1)
        _hash1 = hash(date1)
        assert _hash1 != -1
        date2 = date(2022, 1, 1)
        _hash2 = hash(date2)
        assert _hash1 == _hash2

    @staticmethod
    def test_add_sub():
        du = Duration(days=1)
        dt1 = DateTime(2022, 1, 1)
        dt3 = DateTime(2022, 1, 2)
        # test add
        assert type(dt1 + du) == DateTime
        assert dt1 + du == dt3
        assert du + dt1 == dt3
        assert type(dt1.add(days=1)) == DateTime
        assert dt1.add(days=1) == dt3
        # test sub
        dt3 = DateTime(2021, 12, 31)
        assert type(dt1 - du) == DateTime
        assert dt1 - du == dt3
        assert dt1 - dt3 == du
        assert type(dt1.subtract(days=1)) == DateTime
        assert dt1.subtract(days=1) == dt3

    @staticmethod
    def test_now():
        now1 = DateTime.now()
        now2 = pendulum.DateTime.now()
        assert now1.year == now2.year
        assert now1.month == now2.month

    @staticmethod
    def test_span_add_sub():
        span1 = DateSpan(Date(2022, 1, 1), Date(2022, 1, 4))
        span2 = DateSpan(Date(2022, 1, 4), Date(2022, 1, 5))
        span3 = DateSpan(Date(2022, 1, 1), Date(2022, 1, 5))
        span4 = DateSpan(Date(2022, 1, 3), Date(2022, 1, 8))
        span5 = DateSpan(Date(2022, 1, 1), Date(2022, 1, 8))
        assert span1.add(span2) == span3
        assert span2.add(span1) == span3
        assert span3.subtract(span1)[0] == span2
        assert span3.subtract(span2)[0] == span1
        assert span1.add(span4) == span5

    @staticmethod
    def test_timestamp():
        dt = DateTime(2022, 1, 1)
        dt.timestamp()
        assert True  # when dt.timestamp works, assert true

    @staticmethod
    def test_compatible_2_pandas():
        try:
            dt = DateTime(2022, 1, 1)
            df = DataFrame({"dt": [dt]})
            ts = df.loc[0, "dt"]
            assert True  # when DataFrame() works, assert True
        except Exception as e:
            assert False
