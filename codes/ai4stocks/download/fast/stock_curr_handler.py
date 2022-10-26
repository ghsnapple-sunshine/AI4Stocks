import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import META_COLS, STOCK_REALTIME_TABLE
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq, MysqlOperator
from ai4stocks.download.fast.fast_handler_base import FastHandlerBase


class StockCurrHandler(FastHandlerBase):
    def __init__(self, operator: MysqlOperator):
        super().__init__(operator)

    def download_and_save(self) -> DataFrame:
        return super().download_and_save()

    # 接口：stock_zh_a_spot_em
    # 此处__download__不需要参数
    def __download__(self) -> DataFrame:
        curr_info = ak.stock_zh_a_spot_em()
        return curr_info

    def __save_to_database__(self, df: DataFrame) -> None:
        if (not isinstance(df, DataFrame)) or df.empty:
            return

        cols = [
            ['datetime', MysqlColType.DATETIME, MysqlColAddReq.KEY],
            ['open', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['close', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['high', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['low', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['chengjiaoliang', MysqlColType.INT32, MysqlColAddReq.NONE],
            ['chengjiaoe', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhenfu', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhangdiefu', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhangdiee', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['huanshoulv', MysqlColType.FLOAT, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=META_COLS)
        self.operator.create_table(name=STOCK_REALTIME_TABLE, col_meta=table_meta)
        self.operator.try_insert_data(name=STOCK_REALTIME_TABLE, data=df)

    def get_table(self) -> DataFrame:
        df = self.operator.get_table(STOCK_REALTIME_TABLE)
        df['code'] = df['datetime'].apply(lambda x: DateTime(x))
        return df
