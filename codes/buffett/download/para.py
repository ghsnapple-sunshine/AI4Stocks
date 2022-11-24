from __future__ import annotations

from typing import Optional, Union

from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import Series, DataFrame, pd
from buffett.adapter.pendulum import date
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN, START_DATE, END_DATE
from buffett.common.constants.col.target import (
    CODE,
    NAME,
    CONCEPT_CODE,
    CONCEPT_NAME,
    INDUSTRY_CODE,
    INDUSTRY_NAME,
    INDEX_CODE,
    INDEX_NAME,
)
from buffett.common.magic import get_attr_safe
from buffett.common.pendulum import DateSpan
from buffett.common.target import Target
from buffett.download.types import HeadType, CombType, FuquanType, FreqType, SourceType


class Para:
    def __init__(
        self,
        target: Optional[Target] = None,
        comb: Optional[CombType] = None,
        span: Optional[DateSpan] = None,
        heads: Optional[list[HeadType]] = None,
    ):
        """
        初始化Para for Handler

        :param target:      标的信息
        :param comb:        复合类型
        :param span:        指定的时间周期
        :param heads:       指定的数据列
        """
        self._target = target
        self._comb = comb
        self._span = span
        self._heads = heads

    @classmethod
    def from_series(cls, series: Series) -> Para:
        """
        从Series中创建para

        :param series:
        :return:
        """
        left = DataFrame(
            index=[
                CODE,
                NAME,
                CONCEPT_CODE,
                CONCEPT_NAME,
                INDUSTRY_CODE,
                INDUSTRY_NAME,
                INDEX_CODE,
                INDEX_NAME,
                FREQ,
                SOURCE,
                FUQUAN,
                START_DATE,
                END_DATE,
            ]
        )
        right = DataFrame({0: series})
        merge = pd.merge(left, right, how="left", left_index=True, right_index=True)
        merge = merge.replace({NAN: None})
        (
            code,
            name,
            concept_code,
            concept_name,
            industry_code,
            industry_name,
            index_code,
            index_name,
            freq,
            source,
            fuquan,
            start_date,
            end_date,
        ) = merge[0]
        return cls._create_para(
            code,
            name,
            concept_code,
            concept_name,
            industry_code,
            industry_name,
            index_code,
            index_name,
            freq,
            fuquan,
            source,
            start_date,
            end_date,
        )

    @classmethod
    def from_tuple(cls, tup: tuple) -> Para:
        """
        从tuple中创建para

        :param tup:
        :return:
        """
        (
            code,
            name,
            concept_code,
            concept_name,
            industry_code,
            industry_name,
            index_code,
            index_name,
            freq,
            source,
            fuquan,
            start_date,
            end_date,
        ) = (
            get_attr_safe(tup, x)
            for x in (
                CODE,
                NAME,
                CONCEPT_CODE,
                CONCEPT_NAME,
                INDUSTRY_CODE,
                INDUSTRY_NAME,
                INDEX_CODE,
                INDEX_NAME,
                FREQ,
                SOURCE,
                FUQUAN,
                START_DATE,
                END_DATE,
            )
        )
        return cls._create_para(
            code,
            name,
            concept_code,
            concept_name,
            industry_code,
            industry_name,
            index_code,
            index_name,
            freq,
            fuquan,
            source,
            start_date,
            end_date,
        )

    @classmethod
    def _create_para(
        cls,
        code,
        name,
        concept_code,
        concept_name,
        industry_code,
        industry_name,
        index_code,
        index_name,
        freq,
        fuquan,
        source,
        start_date,
        end_date,
    ) -> Para:
        target = Target(code=code, name=name)
        if target is None:
            target = Target(code=concept_code, name=concept_name)
        if target is None:
            target = Target(code=industry_code, name=industry_name)
        if target is None:
            target = Target(code=index_code, name=index_name)
        comb = CombType(freq=freq, source=source, fuquan=fuquan)
        span = DateSpan(start=start_date, end=end_date)
        return Para(target=target, comb=comb, span=span)

    def with_target(self, target: Target, condition: bool = True) -> Para:
        """
        条件设置target并返回自身

        :param target:      标的
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._target = target
        return self

    def with_code(self, code: str, condition: bool = True) -> Para:
        """
        条件设置target.code并返回自身

        :param code:        标的代码
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._target is None:
                self._target = Target(code=code)
            else:
                self._target.with_code(code)
        return self

    def with_name(self, name: str, condition: bool = True) -> Para:
        """
        条件设置target.name并返回自身

        :param name:        标的名称
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._target is None:
                self._target = Target(name=name)
            else:
                self._target.with_name(name)
        return self

    def with_comb(self, comb: CombType, condition: bool = True) -> Para:
        """
        条件设置comb并返回自身

        :param comb:        复合类型
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._comb = comb
        return self

    def with_fuquan(self, fuquan: FuquanType, condition: bool = True) -> Para:
        """
        条件设置comb.fuquan并返回自身

        :param fuquan:      复权类型
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._comb is None:
                self._comb = CombType(fuquan=fuquan)
            else:
                self._comb.with_fuquan(fuquan)
        return self

    def with_source(self, source: SourceType, condition: bool = True) -> Para:
        """
        条件设置comb.source并返回自身

        :param source:      数据源类型
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._comb is None:
                self._comb = CombType(source=source)
            else:
                self._comb.with_source(source)
        return self

    def with_freq(self, freq: FreqType, condition: bool = True) -> Para:
        """
        条件设置comb.freq并返回自身

        :param freq:        复权类型
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._comb is None:
                self._comb = CombType(freq=freq)
            else:
                self._comb.with_freq(freq)
        return self

    def with_span(self, span: DateSpan, condition: bool = True) -> Para:
        """
        条件设置datespan并返回自身

        :param span:    指定的时间周期
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._span = span
        return self

    def with_start(self, start: Optional[date], condition: bool = True) -> Para:
        """
        条件设置datespan.start并返回自身

        :param start:       指定的开始时间
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._span is None:
                self._span = DateSpan(start=start)
            else:
                self._span.with_start(start)
        return self

    def with_end(self, end: Optional[date], condition: bool = True) -> Para:
        """
        条件设置datespan.end并返回自身

        :param end:         指定的结束时间
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._span is None:
                self._span = DateSpan(end=end)
            else:
                self._span.with_end(end)
        return self

    def with_start_n_end(
        self, start: Optional[date], end: Optional[date], condition: bool = True
    ):
        """
        条件设置datespan.start和datespan.end并返回自身

        :param start:       指定的开始时间
        :param end:         指定的结束时间
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._span = DateSpan(start=start, end=end)
        return self

    def with_heads(self, heads: list[HeadType], condition: bool = True) -> Para:
        """
        条件设置heads并返回自身

        :param heads:       指定的数据列
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._heads = heads
        return self

    def clone(self) -> Para:
        """
        复制自身

        :return:            复制的对象
        """
        return Para(
            target=None if self._target is None else self._target.clone(),
            comb=None if self._comb is None else self._comb.clone(),
            span=None if self._span is None else self._span.clone(),
            heads=None if self._heads is None else self._heads.copy(),
        )

    @property
    def target(self) -> Optional[Target]:
        return self._target

    @property
    def comb(self) -> Optional[CombType]:
        return self._comb

    @property
    def span(self) -> Optional[DateSpan]:
        return self._span

    @property
    def heads(self) -> Optional[list[HeadType]]:
        return self._heads
