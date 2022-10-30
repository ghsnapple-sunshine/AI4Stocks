from buffett.common.pendelum.datespan import DateSpan
from buffett.common.pendelum.datetime import DateTime
from buffett.common.pendelum.tools import datetime_to_date


class TimeSpan:
    # TimeSpan所描述的范围为[start, end]
    def __init__(self, start: DateTime, end: DateTime):
        """
        初始化TimeSpan

        :param start:       开始日期
        :param end:         结束日期
        """
        if not isinstance(start, DateTime):
            raise ValueError('Invalid start for datespan')
        if not isinstance(end, DateTime):
            raise ValueError('Invalid end for datespan')
        if start > end:
            raise ValueError('Invalid datespan when start earlier than end')

        self._start = start
        self._end = end

    @property
    def start(self) -> DateTime:
        return self._start

    @property
    def end(self) -> DateTime:
        return self._end

    def to_datespan(self) -> DateSpan:
        return DateSpan(start=datetime_to_date(self._start),
                        end=datetime_to_date(self._end))
