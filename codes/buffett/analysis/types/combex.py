from __future__ import annotations

from typing import Optional

from buffett.analysis.types import AnalystType
from buffett.download.types import CombType, FuquanType, SourceType, FreqType


class CombExType(CombType):
    def __new__(
        cls,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
        analysis: Optional[AnalystType] = None,
    ):
        if any([fuquan, source, freq, analysis]):
            return super(CombType, cls).__new__(cls)
        return None

    def __init__(
        self,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
        analysis: Optional[AnalystType] = None,
    ):
        super(CombExType, self).__init__(source=source, freq=freq, fuquan=fuquan)
        self._analysis = analysis

    def with_fuquan(self, fuquan: FuquanType) -> CombExType:
        """
        设置fuquan并返回自身

        :param fuquan:      复权类型
        :return:            Self
        """
        self._fuquan = fuquan
        return self

    def with_source(self, source: SourceType) -> CombExType:
        """
        设置source并返回自身

        :param source:      数据来源
        :return:            Self
        """
        self._source = source
        return self

    def with_freq(self, freq: FreqType) -> CombExType:
        """
        设置freq并返回自身

        :param freq:        数据频率
        :return:            Self
        """
        self._freq = freq
        return self

    def with_analysis(self, analysis: AnalystType) -> CombExType:
        """
        设置fuquan并返回自身

        :param analysis:    分析类型
        :return:            Self
        """
        self._analysis = analysis
        return self

    def clone(self) -> CombExType:
        """
        复制自身

        :return:            复制的对象
        """
        return CombExType(
            source=self._source,
            fuquan=self._fuquan,
            freq=self._freq,
            analysis=self._analysis,
        )

    def __eq__(self, other):
        if isinstance(other, CombExType):
            return (
                self._freq == other.freq
                and self._source == other.source
                and self._fuquan == other.fuquan
                and self._analysis == other.analysis
            )
        return False

    def __hash__(self) -> int:
        return (
            hash(self._freq)
            ^ hash(self._source)
            ^ hash(self._fuquan)
            ^ hash(self._analysis)
        )

    @property
    def source(self) -> Optional[SourceType]:
        return self._source

    @property
    def fuquan(self) -> Optional[FuquanType]:
        return self._fuquan

    @property
    def freq(self) -> Optional[FreqType]:
        return self._freq

    @property
    def analysis(self) -> Optional[AnalystType]:
        return self._analysis

    @classmethod
    def from_base(cls, comb: Optional[CombType]) -> Optional[CombExType]:
        """
        基于base转换

        :param comb:
        :return:
        """
        if comb is None:
            return None
        return CombExType(source=comb.source, freq=comb.freq, fuquan=comb.fuquan)

    @classmethod
    def to_base(cls, comb: Optional[CombExType]) -> Optional[CombType]:
        """
        转换得到base

        :param comb:
        :return:
        """
        if comb is None:
            return None
        return CombType(source=comb.source, freq=comb.freq, fuquan=comb.fuquan)
