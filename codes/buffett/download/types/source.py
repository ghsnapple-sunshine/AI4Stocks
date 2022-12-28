from buffett.common import ComparableEnum


class SourceType(ComparableEnum):
    BS = 1
    AK_DC = 10
    AK_DC_CONCEPT = 11
    AK_DC_INDUSTRY = 12
    AK_DC_INDEX = 13
    AK_LG_PEPB = 20
    AK_TH = 30
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
            SourceType.AK_TH: "th_stock",
            SourceType.ANA: "ana_stock",
            SourceType.ANA_CONCEPT: "ana_concept",
            SourceType.ANA_INDUSTRY: "ana_industry",
            SourceType.ANA_INDEX: "ana_index",
        }
        return SOURCE_TYPE_DICT[self]

    def is_analysis(self) -> bool:
        SOURCE_TYPE_DICT = {
            SourceType.BS: False,
            SourceType.AK_DC: False,
            SourceType.AK_DC_CONCEPT: False,
            SourceType.AK_DC_INDUSTRY: False,
            SourceType.AK_DC_INDEX: False,
            SourceType.AK_LG_PEPB: False,
            SourceType.AK_TH: False,
            SourceType.ANA: True,
            SourceType.ANA_CONCEPT: True,
            SourceType.ANA_INDUSTRY: True,
            SourceType.ANA_INDEX: True,
        }
        return SOURCE_TYPE_DICT[self]

    __str__ = sql_format
