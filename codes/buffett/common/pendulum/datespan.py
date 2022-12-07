from __future__ import annotations

from typing import Optional

from buffett.adapter.numpy import np
from buffett.adapter.pendulum import date
from buffett.common.error import ParamTypeError
from buffett.common.pendulum.convert import convert_date, convert_datetime
from buffett.common.pendulum.date import Date


class DateSpan:
    def __new__(cls, start: Optional[date] = None, end: Optional[date] = None):
        if start is None and end is None:
            return None
        return super(DateSpan, cls).__new__(cls)

    # DateSpan所描述的范围为[start, end)
    def __init__(self, start: Optional[date] = None, end: Optional[date] = None):
        """
        初始化DateSpan

        :param start:       开始日期
        :param end:         结束日期
        """
        is_start_date = isinstance(start, date)
        is_end_date = isinstance(end, date)
        if not is_start_date and start is not None:
            raise ParamTypeError("start", Optional[date])
        if not is_end_date and end is not None:
            raise ParamTypeError("end", Optional[date])

        self._start = convert_date(start)
        self._end = convert_date(end)

        if is_start_date and is_end_date and self._start >= self._end:
            raise ValueError(
                f"Invalid datespan when 'start:{start}' later than or equals 'end:{end}'"
            )

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
            self._start = convert_date(start)
        return self

    def with_end(self, end: Optional[date], condition: bool = True) -> DateSpan:
        """
        条件设置end并返回自身

        :param end:             结束时间
        :param condition:       条件
        :return:                Self
        """
        if condition:
            self._end = convert_date(end)
        return self

    def is_inside(self, dt: Date) -> bool:
        """
        判断日期是否在span内

        :param dt:            日期
        :return:                是否在span内
        """
        dt = convert_datetime(dt)
        condition1 = True if self._start is None else self._start <= dt
        condition2 = True if self._end is None else dt < self._end
        return condition1 and condition2

    is_insides = np.vectorize(is_inside)

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

    def subtract(self, other: Optional[DateSpan]) -> list[DateSpan]:
        """
        求两个DateSpan的差集

        :param other:
        :return:
        """
        if other is None:
            return [self]
        condition1 = other.start <= self._start
        condition2 = other.end >= self._end
        if condition1 and condition2:
            return []
        if condition1 and not condition2:
            return [DateSpan(start=other._end, end=self._end)]
        if not condition1 and condition2:
            return [DateSpan(start=self._start, end=other.start)]
        return [
            DateSpan(start=self._start, end=other.start),
            DateSpan(start=other.end, end=self._end),
        ]

    def add(self, other: Optional[DateSpan]) -> DateSpan:
        """
        求两个dataspan的交集

        :param other:
        :return:
        """
        if other is None:
            return self
        if (other.end >= self._start) == (other.start <= self._end):
            return DateSpan(min(self._start, other.start), max(self._end, other.end))
        raise ValueError("self and other is separated and cannot be added.")

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

    def __hash__(self):
        return hash(self._start) ^ hash(self._end)
