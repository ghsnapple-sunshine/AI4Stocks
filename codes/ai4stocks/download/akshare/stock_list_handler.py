# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from ai4stocks.common.constants import COL_STOCK_CODE, COL_STOCK_NAME, META_COLS, STOCK_LIST_TABLE
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.connect.mysql_common import MysqlColAddReq, MysqlColType
from ai4stocks.download.connect.mysql_operator import MysqlOperator


def __download__() -> DataFrame:
    stocks = ak.stock_info_a_code_name()

    # 剔除4开头的退市股票。
    stocks = stocks[
        stocks['code'].apply(lambda x: x[0] != '4')
    ]

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

    def download_and_save(self) -> DataFrame:
        stocks = __download__()
        self.__save_2_database__(stocks)
        return stocks

    def get_table(self) -> DataFrame:
        stocks = self.op.get_table(STOCK_LIST_TABLE)
        stocks['code'] = stocks['code'].apply(lambda x: StockCode(x))
        return stocks
