from buffett.analysis import Para
from buffett.analysis.study import FuquanAnalyst
from buffett.analysis.study.tools import TableNameTool as AnaTool
from buffett.analysis.types import CombExType, AnalystType
from buffett.common.constants.col import OPEN, CLOSE, HIGH, LOW
from buffett.common.constants.table import TRA_CAL
from buffett.common.pendulum import Date
from buffett.common.target import Target
from buffett.download.handler.stock import (
    DcDailyHandler,
    BsMinuteHandler,
    DcFhpgHandler,
)
from buffett.download.handler.tools import TableNameTool as DlTool
from buffett.download.types import FuquanType, SourceType, FreqType
from test import create_1stock, create_2stocks, create_ex_1stock, DbSweeper
from test.analysis.analysis_tester import AnalysisTester

COMB_ANA_BFQ = CombExType(
    source=SourceType.ANA,
    freq=FreqType.DAY,
    fuquan=FuquanType.BFQ,
    analysis=AnalystType.CONV,
)
COMB_ANA_HFQ = COMB_ANA_BFQ.clone().with_fuquan(FuquanType.HFQ)
COMB_DC_BFQ = COMB_ANA_BFQ.clone().with_source(SourceType.AK_DC)
COMB_DC_HFQ = COMB_ANA_HFQ.clone().with_source(SourceType.AK_DC)


class TestFuquanAnalyst(AnalysisTester):
    """
    测试所有的Analyst
    """

    _fhpg_handler = None
    _daily_handler = None
    _minute_handler = None
    _analyst = None

    @classmethod
    def _setup_oncemore(cls):
        """
        股票清单里有两支股票，但只下载一支股票的分红配股，以测试两种场景

        :return:
        """
        cls._fhpg_handler = DcFhpgHandler(operator=cls._stk_rop)
        cls._daily_handler = DcDailyHandler(operator=cls._stk_rop)
        cls._minute_handler = BsMinuteHandler(operator=cls._stk_rop)
        cls._analyst = FuquanAnalyst(
            stk_rop=cls._stk_rop, ana_rop=cls._ana_rop, ana_wop=cls._ana_wop
        )

    def _setup_always(self) -> None:
        DbSweeper.erase_except(TRA_CAL)

    def test_fuquan(self):
        """
        测试FuquanAnalyst

        :return:
        """
        # 准备数据
        create_1stock(operator=self._stk_rop, source="both")
        self._fhpg_handler.obtain_data()
        create_2stocks(operator=self._stk_rop, source="both")
        self._daily_handler.obtain_data(para=self._long_para)
        self._minute_handler.obtain_data(para=self._long_para)
        # 转换数据
        self._conv_daily_data(code="000001")
        self._conv_daily_data(code="600000")
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
        self._atom_test(code="000023", start=Date(2016, 1, 1), end=Date(2017, 12, 31))

    def test_000686(self):
        """
        现网异常数据
        （除权日有两条记录）

        :return:
        """
        self._atom_test(code="000686", start=Date(2007, 7, 1), end=Date(2007, 8, 31))

    def test_000672(self):
        """
        现网异常数据
        （连续两个交易日除权）

        :return:
        """
        self._atom_test(code="000672", start=Date(2000, 1, 1), end=Date(2020, 12, 31))

    def test_000638(self):
        """
        现网异常数据
        (除权日停牌）

        :return:
        """
        self._atom_test(code="000638", start=Date(2000, 1, 1), end=Date(2020, 12, 31))

    def test_001202(self):
        """
        现网异常数据
        (区间内无数据）

        :return:
        """
        self._atom_test(code="001202", start=Date(2000, 1, 1), end=Date(2021, 12, 31))

    def test_301127(self):
        """
        现网异常数据
        (只有一条数据）

        :return:
        """
        self._atom_test(code="301127", start=Date(2000, 1, 1), end=Date(2021, 12, 31))

    def test_000605(self):
        """
        现网异常数据
        （2000/1-2000/6缺失数据）

        :return:
        """
        self._atom_test(code="000605", start=Date(2000, 1, 1), end=Date(2021, 12, 31))

    def _atom_test(self, code: str, start: Date, end: Date):
        """
        原子测试

        :param code:
        :param start:
        :param end:
        :return:
        """
        create_ex_1stock(operator=self._operator, target=Target(code), source="both")
        self._fhpg_handler.obtain_data()
        para = (
            Para().with_code(code).with_comb(COMB_ANA_HFQ).with_start_n_end(start, end)
        )
        self._daily_handler.obtain_data(para=para)
        # 转换数据
        self._conv_daily_data(code=code)
        # 测试计算复权因子
        self._analyst.calculate(span=para.span)

    def _conv_daily_data(self, code: str):
        # 转换bfq
        name = AnaTool.get_by_code(Para().with_code(code).with_comb(COMB_ANA_BFQ))
        source_name = DlTool.get_by_code(Para().with_code(code).with_comb(COMB_DC_BFQ))
        self._operator.create_table_from(name=name, source_name=source_name)
        # 转换hfq
        name = AnaTool.get_by_code(Para().with_code(code).with_comb(COMB_ANA_HFQ))
        source_name = DlTool.get_by_code(Para().with_code(code).with_comb(COMB_DC_HFQ))
        self._operator.create_table_from(name=name, source_name=source_name)
