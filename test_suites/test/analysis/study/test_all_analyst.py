from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.handler.index import DcIndexDailyHandler
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.list import SseStockListHandler
from buffett.download.handler.stock import (
    DcDailyHandler,
    BsDailyHandler,
    ThDailyHandler,
    BsMinuteHandler,
    DcFhpgHandler
)
from buffett.download.maintain import StockDailyMaintain
from test import (
    create_1index,
    create_1industry,
    create_1concept,
    create_2stocks,
)
from test.analysis.analysis_tester import AnalysisTester
from test.analysis.study.conv_day import test_conv_day
from test.analysis.study.conv_min import test_conv_min
from test.analysis.study.fuquan_factor import test_fuquan_factor
from test.analysis.study.pattern import test_pattern
from test.analysis.study.stat import test_stat


class TestAnalyst(AnalysisTester):
    """
    测试所有的Analyst
    """

    _calendar_handler = None
    _dc_daily_handler = None
    _bs_daily_handler = None
    _th_daily_handler = None
    _bs_minute_handler = None
    _index_handler = None
    _concept_handler = None
    _industry_handler = None
    _fhpg_handler = None
    _daily_mtain = None

    @classmethod
    def _setup_oncemore(cls):
        # 下载交易日历
        cls._calendar_handler = CalendarHandler(operator=cls._datasource_op)
        cls._calendar_handler.obtain_data()
        # 下载股票清单\日线\分钟线
        create_2stocks(operator=cls._operator, source="both")
        cls._dc_daily_handler = DcDailyHandler(operator=cls._datasource_op)
        cls._dc_daily_handler.obtain_data(para=cls._long_para)
        cls._bs_daily_handler = BsDailyHandler(operator=cls._datasource_op)
        cls._bs_daily_handler.obtain_data(para=cls._long_para)
        cls._th_daily_handler = ThDailyHandler(
            operator=cls._datasource_op,
            target_list_handler=SseStockListHandler(operator=cls._datasource_op),
        )
        cls._th_daily_handler.obtain_data(para=cls._long_para)
        cls._bs_minute_handler = BsMinuteHandler(operator=cls._datasource_op)
        cls._bs_minute_handler.obtain_data(para=cls._long_para)
        # 下载指数数据
        create_1index(operator=cls._datasource_op)
        cls._index_handler = DcIndexDailyHandler(operator=cls._datasource_op)
        cls._index_handler.obtain_data(para=cls._long_para)
        # 下载概念数据
        create_1concept(operator=cls._datasource_op)
        cls._concept_handler = DcConceptDailyHandler(operator=cls._datasource_op)
        cls._concept_handler.obtain_data(para=cls._long_para)
        # 下载行业数据
        create_1industry(operator=cls._datasource_op)
        cls._industry_handler = DcIndustryDailyHandler(
            operator=cls._datasource_op
        ).obtain_data(para=cls._long_para)
        # 下载分红配股数据
        cls._fhpg_handler = DcFhpgHandler(operator=cls._datasource_op)
        cls._fhpg_handler.obtain_data()
        # daily maintain
        cls._daily_mtain = StockDailyMaintain(operator=cls._datasource_op)
        cls._daily_mtain.run()

    def _setup_always(self) -> None:
        pass

    def test_1_conv_day(self):
        return test_conv_day(self)

    def test_2_fuquan_factor(self):
        return test_fuquan_factor(self)

    def test_3_conv_min(self):
        return test_conv_min(self)

    def test_4_pattern(self):
        return test_pattern(self)

    def test_5_stat(self):
        return test_stat(self)
