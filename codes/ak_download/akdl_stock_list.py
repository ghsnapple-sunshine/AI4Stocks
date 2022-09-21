# 对akshare的接口进行封装
# https://www.akshare.xyz/tutorial.html
import akshare as ak
from pandas import DataFrame

from codes.data_connect.mysql_common import MysqlRole, MysqlConstants, MysqlColumnAddReq, MysqlColumnType
from codes.data_connect.mysql_operator import MysqlOperator


class AkDlStockList:
    @staticmethod
    def Download():
        stocks = ak.stock_info_a_code_name()

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
        return stocks

    @staticmethod
    def Save2Database(stocks, role: MysqlRole.DbStock):
        op = MysqlOperator(role)
        data = [['code', MysqlColumnType.Varchar8, MysqlColumnAddReq.PRIMKEY],
                ['name', MysqlColumnType.Varchar6, MysqlColumnAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.COLUMN_INDEXS)
        op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, df)
        op.InsertData(MysqlConstants.STOCK_LIST_TABLE, stocks)
        op.Disconnect()

    @staticmethod
    def DownloadAndSave(role: MysqlRole.DbStock):
        stocks = AkDlStockList.Download()
        AkDlStockList.Save2Database(stocks, role)
        return stocks.shape[0]
