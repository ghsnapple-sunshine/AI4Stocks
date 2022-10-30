from __future__ import annotations

from buffett.common.pendelum.date import Date


class DateSpan:
    # DateSpan所描述的范围为[start, end]
    def __init__(self, start: Date, end: Date):
        """
        初始化DateSpan

        :param start:       开始日期
        :param end:         结束日期
        """
        if not isinstance(start, Date):
            raise ValueError("'start' is not a date item.")
        if not isinstance(end, Date):
            raise ValueError("'end' is not a date item.")
        if start > end:
            raise ValueError("Invalid datespan when 'start' later than 'end'")

        self._start = start
        self._end = end

    def clone(self) -> DateSpan:
        """
        复制自身

        :return:            复制的对象
        """
        return DateSpan(start=self._start, end=self._end)

    def with_start(self, start: Date, condition: bool = True) -> DateSpan:
        """
        条件设置start并返回自身

        :param start:           开始时间
        :param condition:       条件
        :return:                Self
        """
        if condition:
            if isinstance(start, Date):
                self._start = start
            else:
                raise ValueError('\'start\' is required as a Date object.')
        return self

    def with_end(self, end: Date, condition: bool = True) -> DateSpan:
        """
        条件设置end并返回自身

        :param end:             结束时间
        :param condition:       条件
        :return:                Self
        """
        if condition:
            if isinstance(end, Date):
                self._end = end
            else:
                raise ValueError('\'end\' is required as a Date object.')
        return self

    @property
    def start(self) -> Date:
        return self._start

    @property
    def end(self) -> Date:
        return self._end
