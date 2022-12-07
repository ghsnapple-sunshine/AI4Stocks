from buffett.common import ComparableEnum


class SourceType(ComparableEnum):
    BAOSTOCK = 1
    AKSHARE_DONGCAI = 10
    AKSHARE_DONGCAI_CONCEPT = 11
    AKSHARE_DONGCAI_INDUSTRY = 12
    AKSHARE_DONGCAI_INDEX = 13
    AKSHARE_LGLG_PEPB = 20
    AKSHARE_TONGHUASHUN = 30
    ANALYSIS_STOCK = 40
    ANALYSIS_CONCEPT = 41
    ANALYSIS_INDUSTRY = 42
    ANALYSIS_INDEX = 43

    def sql_format(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: "bs_stock",
            SourceType.AKSHARE_DONGCAI: "dc_stock",
            SourceType.AKSHARE_DONGCAI_CONCEPT: "dc_concept",
            SourceType.AKSHARE_DONGCAI_INDUSTRY: "dc_industry",
            SourceType.AKSHARE_DONGCAI_INDEX: "dc_index",
            SourceType.AKSHARE_LGLG_PEPB: "lg_pepb",
            SourceType.AKSHARE_TONGHUASHUN: "th_stock",
            SourceType.ANALYSIS_STOCK: "analysis_stock",
            SourceType.ANALYSIS_CONCEPT: "analysis_concept",
            SourceType.ANALYSIS_INDUSTRY: "analysis_industry",
            SourceType.ANALYSIS_INDEX: "analysis_index",
        }
        return SOURCE_TYPE_DICT[self]

    def is_dongcai(self) -> bool:
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: False,
            SourceType.AKSHARE_DONGCAI: True,
            SourceType.AKSHARE_DONGCAI_CONCEPT: True,
            SourceType.AKSHARE_DONGCAI_INDUSTRY: True,
            SourceType.AKSHARE_DONGCAI_INDEX: True,
            SourceType.AKSHARE_LGLG_PEPB: False,
            SourceType.AKSHARE_TONGHUASHUN: False,
            SourceType.ANALYSIS_STOCK: False,
            SourceType.ANALYSIS_CONCEPT: False,
            SourceType.ANALYSIS_INDUSTRY: False,
            SourceType.ANALYSIS_INDEX: False,
        }
        return SOURCE_TYPE_DICT[self]

    __str__ = sql_format
