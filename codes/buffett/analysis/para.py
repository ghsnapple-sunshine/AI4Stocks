from __future__ import annotations

from typing import Optional

from buffett.analysis.types import AnalystType
from buffett.analysis.types.combex import CombExType
from buffett.common.constants.col.analysis import ANALYSIS
from buffett.common.magic import get_attr_safe
from buffett.common.pendulum import DateSpan
from buffett.common.target import Target
from buffett.download import Para as DPara
from buffett.download.types import FuquanType, SourceType, FreqType


class Para(DPara):
    """
    Para for Analysis(在Para for Handler的基础上扩展)
    """

    def __init__(
        self,
        target: Optional[Target] = None,
        comb: Optional[CombExType] = None,
        span: Optional[DateSpan] = None,
    ):
        """
        初始化Para for Analysis

        :param target:      标的信息
        :param comb:        复合类型
        :param span:        指定的时间周期
        """
        self._target = target
        self._comb = comb
        self._span = span

    def with_comb(self, comb: CombExType, condition: bool = True) -> Para:
        """
        条件设置comb并返回自身

        :param comb:        增强复合类型
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
                self._comb = CombExType(fuquan=fuquan)
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
                self._comb = CombExType(source=source)
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
                self._comb = CombExType(freq=freq)
            else:
                self._comb.with_freq(freq)
        return self

    def with_analysis(self, analysis: AnalystType, condition: bool = True) -> Para:
        """
        设置analysis并返回自身

        :param analysis:
        :param condition:
        :return:            self
        """
        if condition:
            if self._comb is None:
                self._comb = CombExType(analysis=analysis)
            else:
                self._comb.with_analysis(analysis)
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
        )

    @classmethod
    def from_tuple(cls, tup: tuple) -> Para:
        """
        扩展了基类的from_tuple方法

        :param tup:
        :return:
        """
        para = super(Para, cls).from_tuple(tup)
        para = cls.from_base(para)
        para = para.with_analysis(get_attr_safe(tup, ANALYSIS))
        return para

    @property
    def comb(self) -> Optional[CombExType]:
        return self._comb

    @classmethod
    def from_base(cls, para: Optional[DPara]) -> Optional[Para]:
        """
        基于base转换

        :param para:
        :return:
        """
        if para is None:
            return None
        return Para(
            target=para.target, comb=CombExType.from_base(para.comb), span=para.span
        )

    @classmethod
    def to_base(cls, para: Optional[DPara]) -> Optional[Para]:
        """
        转换得到base

        :param para:
        :return:
        """
        if para is None:
            return None
        return DPara(
            target=para.target, comb=CombExType.to_base(para.comb), span=para.span
        )
