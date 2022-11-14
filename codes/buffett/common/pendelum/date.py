from __future__ import annotations

from buffett.adapter.pendulum import Date as PDate, datetime, timedelta
from buffett.common.pendelum.duration import Duration


class Date(PDate):
    """
    def to_datetime(self):
        return DateTime(self.year, self.month, self.day)
    """

    def __lt__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__lt__(other)
        else:
            return super(Date, self).__lt__(other)

    def __gt__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__gt__(other)
        else:
            return super(Date, self).__gt__(other)

    def __le__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__le__(other)
        else:
            return super(Date, self).__le__(other)

    def __ge__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__ge__(other)
        else:
            return super(Date, self).__ge__(other)

    def __eq__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__eq__(other)
        else:
            return super(Date, self).__eq__(other)

    def __ne__(self, other) -> bool:
        if isinstance(other, datetime):
            _self = datetime(self.year, self.month, self.day)
            return _self.__ne__(other)
        else:
            return super(Date, self).__ne__(other)

    def __hash__(self) -> int:
        return super().__hash__()

    def __add__(self, other) -> Date:
        result = super(Date, self).__add__(other)
        return Date(result.year, result.month, result.day)

    def __sub__(self, other) -> Date:
        result = super(Date, self).__sub__(other)
        if isinstance(other, timedelta):
            result.__class__ = Date
        elif isinstance(other, datetime):
            result.__class__ = Duration
        return result

    def add(self, years=0, months=0, weeks=0, days=0) -> Date:
        result = super(Date, self).add(years, months, weeks, days)
        result.__class__ = Date
        return result

    def subtract(self, years=0, months=0, weeks=0, days=0) -> Date:
        result = super(Date, self).subtract(years, months, weeks, days)
        result.__class__ = Date
        return result

    @classmethod
    def today(cls) -> Date:
        result = PDate.today()
        return Date(result.year, result.month, result.day)

    @classmethod
    def yesterday(cls) -> Date:
        return Date.today().subtract(days=1)
