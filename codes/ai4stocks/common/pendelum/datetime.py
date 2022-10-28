from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Union

from pendulum import DateTime as PDateTime
from pendulum.tz.timezone import Timezone

from ai4stocks.common.pendelum import Date


class DateTime(PDateTime, Date):
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

    def __sub__(self, other) -> DateTime:
        result = super(DateTime, self).__sub__(other)
        result.__class__ = DateTime
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
        result.__class__ = DateTime
        return result

    @classmethod
    def now(cls, tz: Optional[Union[str, Timezone]] = None) -> DateTime:
        """
        获取当前时间

        :return:    当前时间
        """
        result = PDateTime.now()
        result.__class__ = DateTime
        return result
