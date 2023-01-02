from buffett.analysis.types import CombExType, AnalystType
from buffett.common.constants.meta.analysis.definition import (
    CONV_DAILY_META,
    ANALY_EVENT_META,
    ANALY_ZDF_META,
)
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.download.types import SourceType, FreqType, FuquanType

META_DICT = {
    # 日线转换
    CombExType(
        source=SourceType.ANA,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
        analysis=AnalystType.CONV,
    ): CONV_DAILY_META,
    CombExType(
        source=SourceType.ANA,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.CONV,
    ): CONV_DAILY_META,
    # 分钟线转换
    CombExType(
        source=SourceType.ANA,
        freq=FreqType.MIN5,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.CONV,
    ): BS_MINUTE_META,
    # Pattern识别
    CombExType(
        source=SourceType.ANA,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.PATTERN,
    ): ANALY_EVENT_META,
    CombExType(
        source=SourceType.ANA_INDEX,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.PATTERN,
    ): ANALY_EVENT_META,
    CombExType(
        source=SourceType.ANA_CONCEPT,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.PATTERN,
    ): ANALY_EVENT_META,
    CombExType(
        source=SourceType.ANA_INDUSTRY,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.PATTERN,
    ): ANALY_EVENT_META,
    # 涨跌幅统计
    CombExType(
        source=SourceType.ANA,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.STAT_ZDF,
    ): ANALY_ZDF_META,
    CombExType(
        source=SourceType.ANA_INDEX,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.STAT_ZDF,
    ): ANALY_ZDF_META,
    CombExType(
        source=SourceType.ANA_CONCEPT,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.STAT_ZDF,
    ): ANALY_ZDF_META,
    CombExType(
        source=SourceType.ANA_INDUSTRY,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
        analysis=AnalystType.STAT_ZDF,
    ): ANALY_ZDF_META,
}
