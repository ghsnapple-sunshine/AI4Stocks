from __future__ import annotations

from typing import Optional

from buffett.download.types.freq import FreqType
from buffett.download.types.fuquan import FuquanType
from buffett.download.types.source import SourceType


class CombType:
    """
    封装类型，封装了FuquanType, SourceType, FreqType
    """

    def __new__(
        cls,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
    ):
        if fuquan is None and source is None and freq is None:
            return None
        return super(CombType, cls).__new__(cls)

    def __init__(
        self,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
    ):
        self._fuquan = fuquan
        self._source = source
        self._freq = freq

    def with_fuquan(self, fuquan: FuquanType) -> CombType:
        """
        设置fuquan并返回自身

        :param fuquan:      复权类型
        :return:            Self
        """
        self._fuquan = fuquan
        return self

    def with_source(self, source: SourceType) -> CombType:
        """
        设置source并返回自身

        :param source:      数据来源
        :return:            Self
        """
        self._source = source
        return self

    def with_freq(self, freq: FreqType) -> CombType:
        """
        设置freq并返回自身

        :param freq:        数据频率
        :return:            Self
        """
        self._freq = freq
        return self

    def clone(self) -> CombType:
        """
        复制自身

        :return:            复制的对象
        """
        return CombType(source=self._source, fuquan=self._fuquan, freq=self._freq)

    def __eq__(self, other):
        if isinstance(other, CombType):
            return (
                self._freq == other.freq
                and self._source == other.source
                and self._fuquan == other.fuquan
            )
        return False

    def __hash__(self) -> int:
        return hash(self._freq) ^ hash(self._source) ^ hash(self._fuquan)

    @property
    def source(self) -> Optional[SourceType]:
        return self._source

    @property
    def fuquan(self) -> Optional[FuquanType]:
        return self._fuquan

    @property
    def freq(self) -> Optional[FreqType]:
        return self._freq
