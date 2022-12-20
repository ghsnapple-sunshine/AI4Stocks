from buffett.common.constants.meta.handler.definition import (
    BS_DAILY_META,
    BS_MINUTE_META,
    DC_DAILY_META,
    DC_MINUTE_META,
)
from buffett.download.types import CombType, SourceType, FreqType, FuquanType

META_DICT = {
    CombType(
        source=SourceType.BS,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
    ): BS_DAILY_META,
    CombType(
        source=SourceType.BS,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
    ): BS_DAILY_META,
    # CombType(
    #     source=SourceType.BS,
    #     freq=FreqType.DAY,
    #     fuquan=FuquanType.QFQ,
    # ): BS_DAILY_META,
    CombType(
        source=SourceType.BS,
        freq=FreqType.MIN5,
        fuquan=FuquanType.BFQ,
    ): BS_MINUTE_META,
    CombType(
        source=SourceType.AK_DC,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
    ): DC_DAILY_META,
    CombType(
        source=SourceType.AK_DC,
        freq=FreqType.DAY,
        fuquan=FuquanType.HFQ,
    ): DC_DAILY_META,
    # CombType(
    #     source=SourceType.AK_DC,
    #     freq=FreqType.DAY,
    #     fuquan=FuquanType.QFQ,
    # ): DC_DAILY_META,
    CombType(
        source=SourceType.AK_DC,
        freq=FreqType.MIN5,
        fuquan=FuquanType.BFQ,
    ): DC_MINUTE_META,
    CombType(
        source=SourceType.AK_DC_INDEX,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
    ): DC_DAILY_META,
    CombType(
        source=SourceType.AK_DC_CONCEPT,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
    ): DC_DAILY_META,
    CombType(
        source=SourceType.AK_DC_INDUSTRY,
        freq=FreqType.DAY,
        fuquan=FuquanType.BFQ,
    ): DC_DAILY_META,
}
