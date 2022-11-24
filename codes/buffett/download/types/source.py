from buffett.common import ComparableEnum


class SourceType(ComparableEnum):
    BAOSTOCK = 1
    AKSHARE_DONGCAI = 10
    AKSHARE_DONGCAI_CONCEPT = 11
    AKSHARE_DONGCAI_INDUSTRY = 12
    AKSHARE_DONGCAI_INDEX = 13
    AKSHARE_LGLG_PEPB = 20
    AKSHARE_TONGHUASHUN = 100

    def sql_format(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: "bs_stock",
            SourceType.AKSHARE_DONGCAI: "dc_stock",
            SourceType.AKSHARE_DONGCAI_CONCEPT: "dc_concept",
            SourceType.AKSHARE_DONGCAI_INDUSTRY: "dc_industry",
            SourceType.AKSHARE_DONGCAI_INDEX: "dc_index",
            SourceType.AKSHARE_LGLG_PEPB: "lg_pepb",
            SourceType.AKSHARE_TONGHUASHUN: "th_stock",
        }
        return SOURCE_TYPE_DICT[self]

    def __str__(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: "bs(股票)",
            SourceType.AKSHARE_DONGCAI: "ak(东财,股票)",
            SourceType.AKSHARE_DONGCAI_CONCEPT: "ak(东财,概念)",
            SourceType.AKSHARE_DONGCAI_INDUSTRY: "ak(东财,行业)",
            SourceType.AKSHARE_DONGCAI_INDEX: "ak(东财,指数)",
            SourceType.AKSHARE_LGLG_PEPB: "ak(乐股,PEPB)",
            SourceType.AKSHARE_TONGHUASHUN: "ak(同花顺)",
        }
        return SOURCE_TYPE_DICT[self]
