from buffett.adapter.pandas import DataFrame, pd
from buffett.common import Code
from buffett.common.pendelum import Date
from buffett.constants.col.stock import CODE, NAME
from buffett.download import Para
from buffett.download.fast import StockListHandler as SHandler
from buffett.download.manage.table_manager import TableManager
from buffett.download.slow import AkDailyHandler as AHandler, BsMinuteHandler as BHandler
from buffett.download.slow.ak_daily_handler import _META as A_META
from buffett.download.slow.bs_minute_handler import _META as B_META
from buffett.download.slow.table_name import TableNameTool
from buffett.download.types import FreqType, SourceType, FuquanType
from test import Tester, DbSweeper


class TableManagerTest(Tester):
    def setUp(self) -> None:
        super(TableManagerTest, self).setUp()
        # 清理数据库
        DbSweeper.cleanup()
        # 初始化StockList
        data = [['000001', '平安银行']]
        SHandler(operator=self.operator)._save_to_database(
            df=DataFrame(data=data, columns=[CODE, NAME]))

    def test_ak_daily(self):
        # 下载日线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        AHandler(operator=self.operator).obtain_data(para=para)
        para = para.with_code(Code('000001')) \
            .with_freq(FreqType.DAY) \
            .with_source(SourceType.AKSHARE_DONGCAI) \
            .with_fuquan(FuquanType.BFQ)
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = TableManager(self.operator).get_meta(name=table_name)
        rem = pd.concat([meta_get, A_META]).drop_duplicates(keep=False)
        assert rem.empty

    def test_bs_minute(self):
        # 下载分钟线数据
        para = Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 5))
        BHandler(operator=self.operator).obtain_data(para=para)
        para = para.with_code(Code('000001')) \
            .with_freq(FreqType.MIN5) \
            .with_source(SourceType.BAOSTOCK) \
            .with_fuquan(FuquanType.BFQ)
        table_name = TableNameTool.get_by_code(para=para)
        meta_get = TableManager(self.operator).get_meta(name=table_name)
        rem = pd.concat([meta_get, B_META]).drop_duplicates(keep=False)
        assert rem.empty
