from buffett.adapter.pendulum import Date
from buffett.download import Para
from buffett.download.handler.tools import TableNameTool
from buffett.download.types import SourceType, FuquanType, FreqType
from test import SimpleTester


class TestTableName(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    @staticmethod
    def test_get_by_code_bfq():
        para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.AK_DC)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_stock_dayinfo_000001_"

    @staticmethod
    def test_get_by_code_hfq():
        para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.AK_DC)
            .with_fuquan(FuquanType.HFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_stock_dayinfo_000001_hfq"

    @staticmethod
    def test_get_by_code_qfq():
        para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.AK_DC)
            .with_fuquan(FuquanType.QFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_stock_dayinfo_000001_qfq"

    @staticmethod
    def test_get_by_code_bfq_min5():
        para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.BS)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.MIN5)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "bs_stock_min5info_000001_"

    @staticmethod
    def test_get_by_code_index():
        para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.AK_DC_INDEX)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_index_dayinfo_000001_"

    @staticmethod
    def test_get_by_code_industry():
        para = (
            Para()
            .with_code("BK1039")
            .with_source(SourceType.AK_DC_INDUSTRY)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_industry_dayinfo_BK1039_"

    @staticmethod
    def test_get_by_code_concept():
        para = (
            Para()
            .with_code("BK0493")
            .with_source(SourceType.AK_DC_CONCEPT)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_code(para=para)
        assert table_name == "dc_concept_dayinfo_BK0493_"

    @staticmethod
    def test_get_by_date():
        para = (
            Para()
            .with_start(Date(2022, 1, 1))
            .with_source(SourceType.AK_DC)
            .with_fuquan(FuquanType.BFQ)
            .with_freq(FreqType.DAY)
        )
        table_name = TableNameTool.get_by_date(para=para)
        assert table_name == "dc_stock_dayinfo_2022_01_"
