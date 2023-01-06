from unittest.mock import patch

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.recorder import AnalysisRecorder
from buffett.analysis.study import ConvertStockDailyAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import OPEN, CLOSE, HIGH, LOW, CJL, DATE
from buffett.common.constants.col.my import USE, DC
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.magic import get_name
from buffett.common.pendulum import DateSpan, Date
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.maintain import StockDailyMaintain
from buffett.download.types import FreqType, SourceType, FuquanType
from system.mock_tester import MockTester


class TestConvertStockDailyAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = ConvertStockDailyAnalyst(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000001(self):
        """
        测试流程

        :return:
        """
        stock_list = DataFrame({CODE: ["000001"], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ):
            with patch.object(
                BsStockListHandler,
                get_name(BsStockListHandler.select_data),
                return_value=stock_list,
            ):
                with patch.object(
                    AnalysisRecorder,
                    get_name(AnalysisRecorder.select_data),
                    return_value=None,
                ):
                    with patch.object(
                        StockDailyMaintain,
                        get_name(StockDailyMaintain.select_data),
                        return_value=DataFrame(
                            {DATE: [Date(1991, 4, 3)], CODE: ["000001"], USE: [DC]}
                        ),
                    ):
                        self._analyst.calculate(
                            span=DateSpan(start=Date(1990, 1, 1), end=Date(2022, 12, 1))
                        )
        table_name = TableNameTool.get_by_code(
            para=Para()
            .with_code("000001")
            .with_source(SourceType.ANA)
            .with_freq(FreqType.DAY)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.CONV)
        )
        data = self._operator.select_data(name=table_name, meta=None)
        assert all(
            [
                pd.notna(x)
                for x in data[[OPEN, CLOSE, HIGH, LOW, CJL]].values.reshape(
                    (data.shape[0] * 5,)
                )
            ]
        )

    def test_000003(self):
        """
        无法计算得出结果

        :return:
        """
        stock_list = DataFrame({CODE: ["000003"], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ):
            with patch.object(
                BsStockListHandler,
                get_name(BsStockListHandler.select_data),
                return_value=stock_list,
            ):
                with patch.object(
                    AnalysisRecorder,
                    get_name(AnalysisRecorder.select_data),
                    return_value=None,
                ):
                    self._analyst.calculate(
                        span=DateSpan(start=Date(1990, 1, 1), end=Date(2022, 12, 1))
                    )
        table_name = TableNameTool.get_by_code(
            para=Para()
            .with_code("000003")
            .with_source(SourceType.ANA)
            .with_freq(FreqType.DAY)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.CONV)
        )
        data = self._operator.select_data(name=table_name, meta=None)
        assert all(
            [
                pd.notna(x)
                for x in data[[OPEN, CLOSE, HIGH, LOW, CJL]].values.reshape(
                    (data.shape[0] * 5,)
                )
            ]
        )
