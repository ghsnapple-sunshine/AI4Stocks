from buffett.analysis.study import FuquanAnalyst
from buffett.common.constants.col import OPEN, CLOSE, HIGH, LOW
from buffett.download.handler.stock import DcDailyHandler, BsMinuteHandler
from buffett.download.handler.stock.dc_fhpg import DcFhpgHandler
from buffett.download.types import FuquanType, SourceType, FreqType
from test import create_1stock, create_2stocks
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
        create_1stock(operator=cls._operator, source="both")
        cls._fhpg_handler = DcFhpgHandler(operator=cls._operator)
        cls._fhpg_handler.obtain_data()
        create_2stocks(operator=cls._operator)
        create_2stocks(operator=cls._operator, source="bs")
        cls._daily_handler = DcDailyHandler(operator=cls._operator)
        cls._minute_handler = BsMinuteHandler(operator=cls._operator)
        cls._daily_handler.obtain_data(para=cls._long_para)
        cls._minute_handler.obtain_data(para=cls._long_para)
        cls._analyst = FuquanAnalyst(
            datasource_op=cls._select_op, operator=cls._insert_op
        )

    def _setup_always(self) -> None:
        pass

    def test_fuquan(self):
        """
        测试FuquanAnalyst

        :return:
        """
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
