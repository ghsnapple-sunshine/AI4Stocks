# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from ai4stocks.common import COL_STOCK_CODE, COL_STOCK_NAME, META_COLS, STOCK_LIST_TABLE, StockCode
from ai4stocks.download.connect import MysqlColAddReq, MysqlColType, MysqlOperator
from ai4stocks.download.fast.fast_handler_base import FastHandlerBase


class StockListHandler(FastHandlerBase):
    def __init__(self, operator: MysqlOperator):
        super().__init__(operator=operator)

    """
    def download_and_save(self) -> DataFrame:
        return super().download_and_save()
    """

    def __download__(self) -> DataFrame:
        stocks = ak.stock_info_a_code_name()
        # 剔除三板市场的股票
        stocks = stocks[
            stocks['code'].apply(lambda x: (x[0] != '4') & (x[0] != '8'))
        ]
        return stocks

    def __save_to_database__(self, df: DataFrame) -> None:
        cols = [
            [COL_STOCK_CODE, MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
            [COL_STOCK_NAME, MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(
            data=cols,
            columns=META_COLS)
        self.operator.create_table(STOCK_LIST_TABLE, table_meta)
        self.operator.try_insert_data(STOCK_LIST_TABLE, df)  # 忽略重复Insert
        self.operator.disconnect()

    def get_table(self) -> DataFrame:
        df = self.operator.get_table(STOCK_LIST_TABLE)
        df['code'] = df['code'].apply(lambda x: StockCode(x))
        return df
