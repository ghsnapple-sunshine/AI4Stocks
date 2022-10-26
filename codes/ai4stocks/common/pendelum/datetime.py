from datetime import date

from pendulum import DateTime as PDateTime


class DateTime(PDateTime):
    def to_date(self):
        return date(self.year, self.month, self.day)

    def __lt__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__lt__(_other)
        else:
            return super(DateTime, self).__lt__(other)

    def __gt__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__gt__(_other)
        else:
            return super(DateTime, self).__gt__(other)

    def __le__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__le__(_other)
        else:
            return super(DateTime, self).__le__(other)

    def __ge__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__ge__(_other)
        else:
            return super(DateTime, self).__ge__(other)

    def __eq__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__eq__(_other)
        else:
            return super(DateTime, self).__eq__(other)

    def __ne__(self, other):
        if isinstance(other, date):
            _other = DateTime(other.year, other.month, other.day)
            return super(DateTime, self).__ne__(_other)
        else:
            return super(DateTime, self).__ne__(other)
