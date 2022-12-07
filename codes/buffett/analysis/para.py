from __future__ import annotations

from typing import Optional

from buffett.analysis.types import AnalysisType
from buffett.common.constants.col.analysis import ANALYSIS
from buffett.common.magic import get_attr_safe
from buffett.download import Para as DPara


class Para(DPara):
    """
    Para for Analysis(在Para for Handler的基础上扩展)
    """

    def __init__(self, analysis: Optional[AnalysisType] = None, **kwargs):
        """
        初始化Para for Analysis

        :param analysis:
        :param kwargs:    传递给download.Para的参数
        """
        super(Para, self).__init__(**kwargs)
        self._analysis = analysis

    def with_analysis(self, analysis: AnalysisType) -> Para:
        """
        设置analysis并返回自身

        :param analysis:
        :return:            self
        """
        self._analysis = analysis
        return self

    @property
    def analysis(self) -> AnalysisType:
        """
        得到analysis

        :return:
        """
        return self._analysis

    @classmethod
    def from_tuple(cls, tup: tuple) -> Para:
        """
        扩展了基类的from_tuple方法

        :param tup:
        :return:
        """
        para = super(Para, cls).from_tuple(tup)
        para.__class__ = cls
        para._analysis = get_attr_safe(tup, ANALYSIS)
        return para

    def clone(self) -> Para:
        """
        复制自身

        :return:            复制的对象
        """
        return Para(
            target=None if self._target is None else self._target.clone(),
            comb=None if self._comb is None else self._comb.clone(),
            span=None if self._span is None else self._span.clone(),
            analysis=self._analysis
        )
