from ai4stocks.common.pendelum.datespan import DateSpan
from ai4stocks.common.pendelum.datetime import DateTime


class TimeSpan:
    # TimeSpan所描述的范围为[start, end]
    def __init__(self, start: DateTime, end: DateTime):
        if start > end:
            err_msg = 'Invalid timespan [{0}, {1})'.format(
                start.format('YYYY-MM-DD'), end.format('YYYY-MM-DD'))
            raise ValueError(err_msg)
        self._start = start
        self._end = end

    @property
    def start(self) -> DateTime:
        return self._start

    @property
    def end(self) -> DateTime:
        return self._end

    def to_datespan(self) -> DateSpan:
        start = self._start.to_date()
        end = self._end.to_date()
        return DateSpan(start=start, end=end)
