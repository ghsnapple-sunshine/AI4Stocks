from __future__ import annotations

from buffett.adapter.pandas import Timestamp
from buffett.adapter.pendulum import (
    DateTime as PDateTime,
    Duration as PDuration,
    date,
    datetime,
)
from buffett.common.pendulum.date import Date
from buffett.common.pendulum.duration import Duration


class DateTime(Timestamp, Date):
    def __new__(cls, *args, **kwargs):
        obj = super(DateTime, cls).__new__(cls, *args, **kwargs)
        obj.__class__ = DateTime
        return obj

    def __lt__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__lt__(_other)
        else:
            return super(DateTime, self).__lt__(other)

    def __gt__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__gt__(_other)
        else:
            return super(DateTime, self).__gt__(other)

    def __le__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__le__(_other)
        else:
            return super(DateTime, self).__le__(other)

    def __ge__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__ge__(_other)
        else:
            return super(DateTime, self).__ge__(other)

    def __eq__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__eq__(_other)
        else:
            return super(DateTime, self).__eq__(other)

    def __ne__(self, other) -> bool:
        if isinstance(other, date) & (not isinstance(other, datetime)):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__ne__(_other)
        else:
            return super(DateTime, self).__ne__(other)

    def __add__(self, other) -> DateTime:
        result = (
            PDateTime(
                year=self.year,
                month=self.month,
                day=self.day,
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                microsecond=self.microsecond,
            )
            + other
        )
        if isinstance(result, PDateTime):
            return DateTime(
                year=result.year,
                month=result.month,
                day=result.day,
                hour=result.hour,
                minute=result.minute,
                second=result.second,
                microsecond=result.microsecond,
            )
        else:
            return NotImplemented

    __radd__ = __add__

    def __sub__(self, other) -> DateTime | Duration:
        result = (
            PDateTime(
                year=self.year,
                month=self.month,
                day=self.day,
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                microsecond=self.microsecond,
            )
            - other
        )
        if isinstance(result, PDateTime):
            return DateTime(
                year=result.year,
                month=result.month,
                day=result.day,
                hour=result.hour,
                minute=result.minute,
                second=result.second,
                microsecond=result.microsecond,
            )
        elif isinstance(result, PDuration):
            result.__class__ = Duration
            return result

    def add(
        self,
        years=0,
        months=0,
        weeks=0,
        days=0,
        hours=0,
        minutes=0,
        seconds=0,
        microseconds=0,
    ) -> DateTime:
        result = PDateTime.add(
            self,
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            microseconds=microseconds,
        )
        return DateTime(
            year=result.year,
            month=result.month,
            day=result.day,
            hour=result.hour,
            minute=result.minute,
            second=result.second,
            microsecond=result.microsecond,
        )

    def subtract(
        self,
        years=0,
        months=0,
        weeks=0,
        days=0,
        hours=0,
        minutes=0,
        seconds=0,
        microseconds=0,
    ) -> DateTime:
        result = PDateTime.subtract(
            self,
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            microseconds=microseconds,
        )
        return DateTime(
            year=result.year,
            month=result.month,
            day=result.day,
            hour=result.hour,
            minute=result.minute,
            second=result.second,
            microsecond=result.microsecond,
        )

    @classmethod
    def now(cls, tzinfo=None) -> DateTime:
        """
        获取当前时间

        :return:    当前时间
        """
        # now()获取计算机时间，默认带时区，为了避免问题转换成东8区。
        result = super(DateTime, cls).now()
        result.__class__ = DateTime
        return result

    @classmethod
    def today(cls) -> DateTime:
        result = super(DateTime, cls).today()
        return DateTime(result.year, result.month, result.day)

    def format(self, fmt, locale=None):
        """
        格式化时间字符串

        :param fmt:
        :param locale:
        :return:
        """
        result = PDateTime.format(self, fmt=fmt, locale=locale)
        return result

    def format_YMDHms(
        self, ymd_sep: str = "-", joiner: str = " ", hms_sep: str = ":"
    ) -> str:
        """
        按照YYYYMMDD HHmmss进行格式化

        :param ymd_sep:         年，月，日间的分割字符串
        :param joiner:          日，小时间的分割字符串
        :param hms_sep:         小时，分钟，秒间的分割字符串
        :return:
        """
        format_str = f"YYYY{ymd_sep}MM{ymd_sep}DD{joiner}HH{hms_sep}mm{hms_sep}ss"
        return self.format(format_str)

    def __hash__(self):
        return super(DateTime, self).__hash__()
