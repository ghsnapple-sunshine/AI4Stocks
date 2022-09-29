# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from ai4stocks.common.constants import COL_STOCK_CODE, COL_STOCK_NAME, META_COLS, STOCK_LIST_TABLE
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.connect.mysql_common import MysqlColAddReq, MysqlColType
from ai4stocks.download.connect.mysql_operator import MysqlOperator


def __download__() -> DataFrame:
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


class StockListHandler:
    def __init__(
            self,
            op: MysqlOperator
    ):
        self.op = op

    def __save_2_database__(
            self,
            stocks: DataFrame
    ) -> None:
        cols = [
            [COL_STOCK_CODE, MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
            [COL_STOCK_NAME, MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(
            data=cols,
            columns=META_COLS)
        self.op.create_table(STOCK_LIST_TABLE, table_meta)
        self.op.try_insert_data(STOCK_LIST_TABLE, stocks)  # 忽略重复Insert
        self.op.disconnect()

    def downloadAndSave(self) -> DataFrame:
        stocks = __download__()
        self.__save_2_database__(stocks)
        return stocks

    def getTable(self) -> DataFrame:
        stocks = self.op.get_table(STOCK_LIST_TABLE)
        stocks['code'] = stocks.apply(lambda x: StockCode(x['code']), axis=1)
        return stocks
