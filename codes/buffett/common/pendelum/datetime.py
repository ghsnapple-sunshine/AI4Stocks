from __future__ import annotations

from buffett.adapter.pendulum import DateTime as PDateTime, tzinfo as PTzInfo, date, datetime, timedelta
from buffett.common.pendelum.date import Date
from buffett.common.pendelum.duration import Duration


class DateTime(PDateTime, Date):
    def __new__(cls,
                year: int,
                month: int,
                day: int,
                hour: int = 0,
                minute: int = 0,
                second: int = 0,
                microsecond: int = 0,
                tzinfo: Optional[PTzInfo] = None,
                fold: int = 0):
        return super(DateTime, cls).__new__(
            cls, year, month, day, hour=hour, minute=minute, second=second,
            microsecond=microsecond, tzinfo=None)

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
        result = super(DateTime, self).__add__(other)
        result.__class__ = DateTime
        return result

    __radd__ = __add__

    def __sub__(self, other) -> DateTime | Duration:
        result = super(DateTime, self).__sub__(other)
        if isinstance(other, timedelta):
            result.__class__ = DateTime
        elif isinstance(other, datetime):
            result.__class__ = Duration
        return result

    def add(self,
            years=0,
            months=0,
            weeks=0,
            days=0,
            hours=0,
            minutes=0,
            seconds=0,
            microseconds=0) -> DateTime:
        result = super(DateTime, self).add(years, months, weeks, days, hours, minutes, seconds, microseconds)
        result.__class__ = DateTime
        return result

    def subtract(self,
                 years=0,
                 months=0,
                 weeks=0,
                 days=0,
                 hours=0,
                 minutes=0,
                 seconds=0,
                 microseconds=0) -> DateTime:
        result = super(DateTime, self).subtract(years, months, weeks, days, hours, minutes, seconds, microseconds)
        return DateTime(year=result.year,
                        month=result.month,
                        day=result.day,
                        hour=result.hour,
                        minute=result.minute,
                        second=result.second,
                        microsecond=result.microsecond,
                        tzinfo=None)

    @classmethod
    def now(cls,
            tzinfo: Optional[PTzInfo] = None) -> DateTime:
        """
        获取当前时间

        :return:    当前时间
        """
        # now()获取计算机时间，默认带时区，为了避免问题转换成东8区。
        result = PDateTime.now().in_timezone('Asia/Shanghai')
        return DateTime(year=result.year,
                        month=result.month,
                        day=result.day,
                        hour=result.hour,
                        minute=result.minute,
                        second=result.second,
                        microsecond=result.microsecond,
                        tzinfo=None)

    def format_YMDHms(self,
                      ymd_sep: str = '-',
                      joiner: str = ' ',
                      hms_sep: str = ':') -> str:
        """
        按照YYYYMMDD HHmmss进行格式化

        :param ymd_sep:         年，月，日间的分割字符串
        :param joiner:          日，小时间的分割字符串
        :param hms_sep:         小时，分钟，秒间的分割字符串
        :return:
        """
        format_str = f'YYYY{ymd_sep}MM{ymd_sep}DD{joiner}HH{hms_sep}mm{hms_sep}ss'
        return self.format(format_str)
