from __future__ import annotations

from datetime import date
from typing import Optional

from buffett.common import Code
from buffett.common.pendelum import DateSpan
from buffett.common.stock import Stock
from buffett.download.types import HeadType, CombType, FuquanType, FreqType, SourceType


class Para:
    def __init__(self,
                 stock: Optional[Stock] = None,
                 comb: Optional[CombType] = None,
                 span: Optional[DateSpan] = None,
                 heads: Optional[list[HeadType]] = None):
        """
        初始化Para for Handler

        :param stock:       股票信息
        :param comb:        复合类型
        :param span:        指定的时间周期
        :param heads:        指定的数据列
        """
        self._stock = stock
        self._comb = comb
        self._span = span
        self._heads = heads

    def with_stock(self, stock: Stock, condition: bool = True) -> Para:
        """
        条件设置stock并返回自身

        :param stock:       股票
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._stock = stock
        return self

    def with_code(self, code: Code, condition: bool = True) -> Para:
        """
        条件设置stock.code并返回自身

        :param code:        股票代码
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._stock is None:
                self._stock = Stock(code=code)
            else:
                self._stock.with_code(code)
        return self

    def with_name(self, name: str, condition: bool = True) -> Para:
        """
        条件设置stock.name并返回自身

        :param name:        股票名称
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            if self._stock is None:
                self._stock = Stock(name=name)
            else:
                self._stock.with_name(name)
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

    def with_span(self,
                  span: DateSpan,
                  condition: bool = True) -> Para:
        """
        条件设置datespan并返回自身

        :param span:    指定的时间周期
        :param condition:   条件设置
        :return:            Self
        """
        if condition:
            self._span = span
        return self

    def with_start(self,
                   start: Optional[date],
                   condition: bool = True) -> Para:
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

    def with_end(self,
                 end: Optional[date],
                 condition: bool = True) -> Para:
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

    def with_start_n_end(self,
                         start: Optional[date],
                         end: Optional[date],
                         condition: bool = True):
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
        return Para(stock=None if self._stock is None else self._stock.clone(),
                    comb=None if self._comb is None else self._comb.clone(),
                    span=None if self._span is None else self._span.clone(),
                    heads=None if self._heads is None else self._heads.copy())

    @property
    def stock(self) -> Optional[Stock]:
        return self._stock

    @property
    def comb(self) -> Optional[CombType]:
        return self._comb

    @property
    def span(self) -> Optional[DateSpan]:
        return self._span

    @property
    def heads(self) -> Optional[list[HeadType]]:
        return self._heads
