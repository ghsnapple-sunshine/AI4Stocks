from buffett.common import ComparableEnum


class SourceType(ComparableEnum):
    BS = 1
    AK_DC = 10
    AK_DC_CONCEPT = 11
    AK_DC_INDUSTRY = 12
    AK_DC_INDEX = 13
    AK_LG_PEPB = 20
    AK_TSH = 30
    ANA = 40
    ANA_CONCEPT = 41
    ANA_INDUSTRY = 42
    ANA_INDEX = 43

    def sql_format(self):
        SOURCE_TYPE_DICT = {
            SourceType.BS: "bs_stock",
            SourceType.AK_DC: "dc_stock",
            SourceType.AK_DC_CONCEPT: "dc_concept",
            SourceType.AK_DC_INDUSTRY: "dc_industry",
            SourceType.AK_DC_INDEX: "dc_index",
            SourceType.AK_LG_PEPB: "lg_pepb",
            SourceType.AK_TSH: "th_stock",
            SourceType.ANA: "analysis_stock",
            SourceType.ANA_CONCEPT: "analysis_concept",
            SourceType.ANA_INDUSTRY: "analysis_industry",
            SourceType.ANA_INDEX: "analysis_index",
        }
        return SOURCE_TYPE_DICT[self]

    def is_dongcai(self) -> bool:
        SOURCE_TYPE_DICT = {
            SourceType.BS: False,
            SourceType.AK_DC: True,
            SourceType.AK_DC_CONCEPT: True,
            SourceType.AK_DC_INDUSTRY: True,
            SourceType.AK_DC_INDEX: True,
            SourceType.AK_LG_PEPB: False,
            SourceType.AK_TSH: False,
            SourceType.ANA: False,
            SourceType.ANA_CONCEPT: False,
            SourceType.ANA_INDUSTRY: False,
            SourceType.ANA_INDEX: False,
        }
        return SOURCE_TYPE_DICT[self]

    __str__ = sql_format
