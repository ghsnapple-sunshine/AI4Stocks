# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from ai4stocks.download.connect.mysql_common import MysqlConstants, MysqlColAddReq, MysqlColType
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class StockListHandler:
    @staticmethod
    def Download() -> DataFrame:
        stocks = ak.stock_info_a_code_name()

        '''
        # StockList返回的股票代码是“000001”，“000002”这样的格式
        # 但是stock_zh_a_daily（）函数中，要求代码的格式为“sz000001”，或“sh600001”
        # 即必须有sz或者sh作为前序
        # 因此，通过for循环对股票代码code进行格式变换
        for i in range(len(stocks)):
            temp = stocks.iloc[i, 0]
            if temp[0] == "6":
                temp = "sh" + temp
            elif temp[0] == "0":
                temp = "sz" + temp
            elif temp[0] == "3":
                temp = "sz" + temp
            stocks.iloc[i, 0] = temp
        '''

        return stocks

    @staticmethod
    def Save2Database(stocks, op: MysqlOperator) -> None:
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
            ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, table_meta)
        op.TryInsertData(MysqlConstants.STOCK_LIST_TABLE, stocks) # 忽略重复Insert
        op.Disconnect()

    @staticmethod
    def DownloadAndSave(op: MysqlOperator) -> DataFrame:
        stocks = StockListHandler.Download()
        StockListHandler.Save2Database(stocks, op=op)
        return stocks
