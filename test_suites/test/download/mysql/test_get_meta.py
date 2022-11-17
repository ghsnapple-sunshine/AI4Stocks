from buffett.adapter.pandas import DataFrame
from buffett.common import Code
from buffett.common.constants.col.stock import CODE, NAME
from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.handler.stock import (
    StockListHandler,
    AkDailyHandler,
    BsMinuteHandler,
)
from buffett.download.handler.stock.ak_daily import _META as A_META
from buffett.download.handler.stock.bs_minute import _META as B_META
from buffett.download.handler.tools import TableNameTool
from buffett.download.types import FreqType, SourceType, FuquanType
from test import Tester, DbSweeper


class TestGetMeta(Tester):
    def setUp(self) -> None:
        super(TestGetMeta, self).setUp()
        # 清理数据库
        DbSweeper.cleanup()
        # 初始化StockList
        data = [["000001", "平安银行"]]
        StockListHandler(operator=self.operator)._save_to_database(
            df=DataFrame(data=data, columns=[CODE, NAME])
        )

    def test_ak_daily(self):
        # 下载日线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        AkDailyHandler(operator=self.operator).obtain_data(para=para)
        para = (
            para.with_code(Code("000001"))
            .with_freq(FreqType.DAY)
            .with_source(SourceType.AKSHARE_DONGCAI)
            .with_fuquan(FuquanType.BFQ)
        )
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = self.operator.get_meta(name=table_name)
        assert self.compare_dataframe(meta_get, A_META)

    def test_bs_minute(self):
        # 下载分钟线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        para = (
            para.with_code(Code("000001"))
            .with_freq(FreqType.MIN5)
            .with_source(SourceType.BAOSTOCK)
            .with_fuquan(FuquanType.BFQ)
        )
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = self.operator.get_meta(name=table_name)
        assert self.compare_dataframe(meta_get, B_META)
