from datetime import datetime

from pendulum import Date as PDate
from ai4stocks.common.pendelum.datetime import DateTime


class Date(PDate):
    def to_datetime(self):
        return DateTime(self.year, self.month, self.day)

    def __lt__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__lt__(other)
        else:
            return super(Date, self).__lt__(other)

    def __gt__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__gt__(other)
        else:
            return super(Date, self).__gt__(other)

    def __le__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__le__(other)
        else:
            return super(Date, self).__le__(other)

    def __ge__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__ge__(other)
        else:
            return super(Date, self).__ge__(other)

    def __eq__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__eq__(other)
        else:
            return super(Date, self).__eq__(other)

    def __ne__(self, other):
        if isinstance(other, datetime):
            _self = self.to_datetime()
            return super(DateTime, _self).__ne__(other)
        else:
            return super(Date, self).__ne__(other)

    def __hash__(self):
        return super().__hash__()