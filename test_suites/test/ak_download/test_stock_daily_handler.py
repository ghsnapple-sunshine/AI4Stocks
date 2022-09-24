import unittest

from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.ak_download.stock_daily_handler import StockDailyHandler
from ai4stocks.data_connect.mysql_common import MysqlConstants, MysqlColType, MysqlColAddReq
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockDailyHandlerTest(BaseTest):
    def __int__(self):
        print('init!')

    def setUp(self) -> None:
        super().setUp()
        DbSweeper.CleanUp()

    def test_download(self) -> None:
        stocks = self.CreateStocks()
        start_date = DateTime(2022, 1, 1)
        end_date = DateTime(2022, 6, 30)
        tbls = StockDailyHandler.DownloadAndSave(op=self.op, start_date=start_date, end_date=end_date)
        for tbl in tbls:
            self.op.DropTable(tbl)
        assert stocks.shape[0] * 2 == len(tbls)

    def CreateStocks(self) -> DataFrame:
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
            ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, table_meta)
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.InsertData(MysqlConstants.STOCK_LIST_TABLE, df)
        return df


if __name__ == '__main__':
    unittest.main()
