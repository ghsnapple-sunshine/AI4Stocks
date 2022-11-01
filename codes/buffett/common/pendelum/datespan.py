from __future__ import annotations

from datetime import date
from typing import Union, Optional

from buffett.common.error import ParamTypeError
from buffett.common.pendelum.date import Date
from buffett.common.pendelum.convert import convert_date


class DateSpan:
    # DateSpan所描述的范围为[start, end)
    def __init__(self,
                 start: Optional[date] = None,
                 end: Optional[date] = None):
        """
        初始化DateSpan

        :param start:       开始日期
        :param end:         结束日期
        """
        is_start_date = isinstance(start, date)
        is_end_date = isinstance(end, date)
        if not is_start_date and start is not None:
            raise ParamTypeError('start', Union[date, None])
        if not is_end_date and end is not None:
            raise ParamTypeError('end', Union[date, None])
        if start is None and end is None:
            raise ValueError("param 'start' and 'end' cannot be None at the mean time.")
        elif is_start_date and is_end_date and start >= end:
            raise ValueError("Invalid datespan when 'start' later than or equals 'end'")

        self._start = convert_date(start)
        self._end = convert_date(end)

    def clone(self) -> DateSpan:
        """
        复制自身

        :return:            复制的对象
        """
        return DateSpan(start=self._start, end=self._end)

    def with_start(self, start: Optional[date], condition: bool = True) -> DateSpan:
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

    def with_end(self, end: Optional[date], condition: bool = True) -> DateSpan:
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

    def is_in_span(self, date: Date) -> bool:
        """
        判断日期是否在span内

        :param date:            日期
        :return:                是否在span内
        """
        condition1 = True if self._start is None else self._start <= date
        condition2 = True if self._start is None else date < self._start
        return condition1 and condition2

    def is_cross(self, other: DateSpan) -> bool:
        """
        判断两个span是否相交

        :param other:
        :return:
        """
        # o          s---------e        s----------e
        # s                   s----------e
        #            |------------------|
        # 1  FFFFFFFFTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT
        # 2  TTTTTTTTTTTTTTTTTTTTTTTTTTTTFFFFFFFFFFF
        return (other.end > self._start) == (other.start < self._end)

    def subtract(self, other: DateSpan) -> list[DateSpan]:
        """
        求两个DateSpan的差集

        :param other:
        :return:
        """
        condition1 = other.start <= self._start
        condition2 = other.end >= self._end
        if condition1 and condition2:
            return []
        if condition1 and not condition2:
            return [DateSpan(start=other._end, end=self._end)]
        if not condition1 and condition2:
            return [DateSpan(start=self._start, end=other.start)]
        return [DateSpan(start=self._start, end=other.start),
                DateSpan(start=other.end, end=self._end)]

    def add(self, other: DateSpan) -> DateSpan:
        if (other.end >= self._start) == (other.start <= self._end):
            return DateSpan(min(self._start, other.start), max(self._end, other.end))
        raise ValueError('self and other is separated and cannot be added.')

    def __eq__(self, other):
        if isinstance(other, DateSpan):
            return self._start == other.start and self._end == other.end

    @property
    def start(self) -> Date:
        return self._start

    @property
    def end(self) -> Date:
        return self._end

    def __str__(self):
        return f"({self._start}, {self._end})"
