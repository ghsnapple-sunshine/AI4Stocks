from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.maintain.min_bfq import StockMinuteBfqMaintain
from buffett.analysis.recorder import AnalysisRecorder
from buffett.analysis.study.conv_min_bfq import ConvertStockMinuteBfqAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.analysis import META_DICT
from buffett.common.constants.meta.handler import DAILY_MTAIN_META
from buffett.common.constants.table import DAILY_MTAIN
from buffett.common.magic import get_name
from buffett.common.pendulum import DateSpan, Date
from buffett.common.tools import dataframe_is_valid, dataframe_not_valid
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.types import FreqType, SourceType, FuquanType
from system.mock_tester import MockTester


class TestConvertStockMinuteBfqAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = ConvertStockMinuteBfqAnalyst(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_rop=cls._stk_op
        )
        cls._mtain = StockMinuteBfqMaintain(
            mtain_wop=cls._operator, ana_rop=cls._operator, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000001(self):
        """
        测试现网报错数据
        市场中有NA数据

        :return:
        """
        self._atom_test("000001")

    def test_000166(self):
        """
        测试现网报错数据

        :return:
        """
        self._atom_test("000166")

    def _atom_test(self, code: str):
        stock_list = DataFrame({CODE: [code], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ), patch.object(
            BsStockListHandler,
            get_name(BsStockListHandler.select_data),
            return_value=stock_list,
        ), patch.object(
            AnalysisRecorder,
            get_name(AnalysisRecorder.select_data),
            return_value=None,
        ):
            #
            span = DateSpan(start=Date(1990, 1, 1), end=Date(2022, 12, 1))
            self._analyst.calculate(span=span)
            table_name = TableNameTool.get_by_code(
                para=Para()
                .with_code(code)
                .with_source(SourceType.ANA)
                .with_freq(FreqType.MIN5)
                .with_fuquan(FuquanType.BFQ)
                .with_analysis(AnalystType.CONV)
            )
            data = self._operator.select_data(name=table_name, meta=None)
            assert dataframe_is_valid(data)
            #
            para = (
                Para()
                .with_code(code)
                .with_source(SourceType.ANA)
                .with_freq(FreqType.DAY)
                .with_fuquan(FuquanType.BFQ)
                .with_analysis(AnalystType.CONV)
            )
            table_name = TableNameTool.get_by_code(para)
            meta = META_DICT[para.comb]
            self.copydb(
                table_name=table_name,
                meta=meta,
                source=self._ana_op,
                dest=self._operator,
            )
            self._mtain.run()
            data = self._operator.select_data(name=DAILY_MTAIN, meta=DAILY_MTAIN_META)
            assert dataframe_not_valid(data)
