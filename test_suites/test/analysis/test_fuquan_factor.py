from buffett.adapter.pandas import DataFrame
from buffett.analysis.study.tool import get_fuquan_factor, reform
from buffett.common.constants.col import CLOSE, DATE
from buffett.download.handler.stock import DcDailyHandler
from buffett.download.types import FuquanType
from test import Tester, create_1stock


class TestFuquanFactor(Tester):
    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator)
        DcDailyHandler(operator=cls._operator).obtain_data(para=cls._great_para)

    def _setup_always(self) -> None:
        pass

    def test_000001_hfq_to_bfq(self):
        """
        后复权转不复权

        :return:
        """
        #
        daily_handler = DcDailyHandler(operator=self._operator)
        #
        bfq_data = daily_handler.select_data(
            para=self._great_para.clone().with_fuquan(FuquanType.BFQ)
        )
        bfq_data = bfq_data[CLOSE]
        hfq_data = daily_handler.select_data(
            para=self._great_para.clone().with_fuquan(FuquanType.HFQ)
        )
        hfq_data = hfq_data[CLOSE]
        factor = get_fuquan_factor(bfq_data=bfq_data, hfq_data=hfq_data)
        bfq_data_calc = reform(data=hfq_data, factor=factor, name=CLOSE)
        # 对比
        bfq_data_calc = DataFrame(
            {DATE: bfq_data_calc.index, CLOSE: bfq_data_calc.values}
        )
        bfq_data = DataFrame({DATE: bfq_data.index, CLOSE: bfq_data.values})
        assert self.dataframe_almost_equals(
            bfq_data, bfq_data_calc, join_columns=[DATE], rel_tol=2e-2
        )

    def test_000001_bfq_to_hfq_to_bfq(self):
        #
        daily_handler = DcDailyHandler(operator=self._operator)
        #
        bfq_data = daily_handler.select_data(
            para=self._great_para.clone().with_fuquan(FuquanType.BFQ)
        )
        bfq_data = bfq_data[CLOSE]
        hfq_data = daily_handler.select_data(
            para=self._great_para.clone().with_fuquan(FuquanType.HFQ)
        )
        hfq_data = hfq_data[CLOSE]
        factor = get_fuquan_factor(bfq_data=bfq_data, hfq_data=hfq_data)
        factor1 = [[x[0], x[1], x[4], x[5]] for x in factor]
        hfq_data_calc = reform(data=bfq_data, factor=factor1, name=CLOSE)
        bfq_data_calc = reform(data=hfq_data_calc, factor=factor, name=CLOSE)
        # 对比
        bfq_data_calc = DataFrame(
            {DATE: bfq_data_calc.index, CLOSE: bfq_data_calc.values}
        )
        bfq_data = DataFrame({DATE: bfq_data.index, CLOSE: bfq_data.values})
        assert self.dataframe_almost_equals(
            bfq_data, bfq_data_calc, join_columns=[DATE]
        )
