from buffett.common.constants.meta.handler import BS_MINUTE_META, DC_DAILY_META
from buffett.common.pendulum import Date
from buffett.download import Para
from buffett.download.handler.stock import (
    DcDailyHandler,
    BsMinuteHandler,
)
from buffett.download.handler.tools import TableNameTool
from buffett.download.types import FreqType, SourceType, FuquanType
from test import Tester, create_1stock


class TestGetMeta(Tester):
    @classmethod
    def _setup_oncemore(cls):
        # 初始化StockList
        create_1stock(operator=cls._operator)
        create_1stock(operator=cls._operator, is_sse=False)

    def _setup_always(self) -> None:
        # 可以不用清理数据库
        pass

    def test_ak_daily(self):
        # 下载日线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        DcDailyHandler(operator=self._operator).obtain_data(para=para)
        para = (
            para.with_code("000001")
            .with_freq(FreqType.DAY)
            .with_source(SourceType.AKSHARE_DONGCAI)
            .with_fuquan(FuquanType.BFQ)
        )
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = self._operator.get_meta(name=table_name)
        assert self.compare_dataframe(meta_get, DC_DAILY_META)

    def test_bs_minute(self):
        # 下载分钟线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        BsMinuteHandler(operator=self._operator).obtain_data(para=para)
        para = (
            para.with_code("000001")
            .with_freq(FreqType.MIN5)
            .with_source(SourceType.BAOSTOCK)
            .with_fuquan(FuquanType.BFQ)
        )
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = self._operator.get_meta(name=table_name)
        assert self.compare_dataframe(meta_get, BS_MINUTE_META)
