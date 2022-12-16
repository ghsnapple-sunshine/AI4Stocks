from buffett.adapter.pendulum import Date
from buffett.analysis import Para
from buffett.analysis.study import FuquanAnalyst
from buffett.common.constants.col import OPEN, CLOSE, HIGH, LOW
from buffett.common.target import Target
from buffett.download.handler.stock import DcDailyHandler, BsMinuteHandler
from buffett.download.handler.stock.dc_fhpg import DcFhpgHandler
from buffett.download.types import FuquanType, SourceType, FreqType
from test import create_1stock, create_2stocks, create_ex_1stock, DbSweeper
from test.analysis.analysis_tester import AnalysisTester


class TestFuquanAnalyst(AnalysisTester):
    """
    测试所有的Analyst
    """

    _fhpg_handler = None
    _daily_handler = None
    _minute_handler = None

    @classmethod
    def _setup_oncemore(cls):
        """
        股票清单里有两支股票，但只下载一支股票的分红配股，以测试两种场景

        :return:
        """
        cls._fhpg_handler = DcFhpgHandler(operator=cls._operator)
        cls._daily_handler = DcDailyHandler(operator=cls._operator)
        cls._minute_handler = BsMinuteHandler(operator=cls._operator)
        cls._analyst = FuquanAnalyst(
            datasource_op=cls._select_op, operator=cls._insert_op
        )

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_fuquan(self):
        """
        测试FuquanAnalyst

        :return:
        """
        # 准备数据
        create_1stock(operator=self._operator, source="both")
        self._fhpg_handler.obtain_data()
        create_2stocks(operator=self._operator, source="both")
        self._daily_handler.obtain_data(para=self._long_para)
        self._minute_handler.obtain_data(para=self._long_para)
        # 测试计算复权因子
        self._analyst.calculate(span=self._long_para.span)
        bfq_para = (
            self._great_para.clone()
            .with_source(SourceType.AK_DC)
            .with_freq(FreqType.DAY)
        )
        hfq_para = bfq_para.clone().with_fuquan(FuquanType.HFQ)
        bfq_data = self._daily_handler.select_data(para=bfq_para)
        hfq_data = self._daily_handler.select_data(para=hfq_para)
        # 测试转换天级数据
        conv0 = self._analyst.reform_to_hfq(code="000001", df=bfq_data)
        conv1 = self._analyst.reform_to_bfq(code="000001", df=conv0)
        assert self.dataframe_almost_equals(
            bfq_data[[OPEN, CLOSE, HIGH, LOW]], conv1, on_index=True
        )
        assert self.dataframe_almost_equals(
            hfq_data[[OPEN, CLOSE, HIGH, LOW]], conv0, on_index=True, rel_tol=1e-3
        )  # 与下载的分钟级后复权数据比较
        # 测试转换分钟级数据
        bfq_min_para = (
            bfq_para.clone().with_source(SourceType.BS).with_freq(FreqType.MIN5)
        )
        bfq_min_data = self._minute_handler.select_data(para=bfq_min_para)
        mconv0 = self._analyst.reform_to_hfq(code="000001", df=bfq_min_data)
        mconv1 = self._analyst.reform_to_bfq(code="000001", df=mconv0)
        assert self.dataframe_almost_equals(
            bfq_min_data[[OPEN, CLOSE, HIGH, LOW]], mconv1, on_index=True
        )

    def test_000023(self):
        """
        现网异常数据
        （某个除权周期首尾有相同的收盘价）

        :return:
        """
        # 准备数据
        create_ex_1stock(
            operator=self._operator, target=Target("000023"), source="both"
        )
        self._fhpg_handler.obtain_data()
        para = (
            Para()
            .with_start_n_end(Date(2016, 1, 1), Date(2017, 12, 31))
            .with_code("000023")
            .with_fuquan(FuquanType.BFQ)
        )
        self._daily_handler.obtain_data(para=para)
        # 测试计算复权因子
        self._analyst.calculate(span=para.span)

    def test_000686(self):
        """
        现网异常数据
        （某个除权日有两条除权记录）

        :return:
        """
        # 准备数据
        create_ex_1stock(
            operator=self._operator, target=Target("000686"), source="both"
        )
        self._fhpg_handler.obtain_data()
        para = (
            Para()
            .with_start_n_end(Date(2007, 7, 1), Date(2007, 8, 31))
            .with_code("000686")
            .with_fuquan(FuquanType.BFQ)
        )
        self._daily_handler.obtain_data(para=para)
        # 测试计算复权因子
        self._analyst.calculate(span=self._great_para.span)
