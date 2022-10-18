import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import META_COLS, FuquanType, StockCode
from ai4stocks.download import BaseHandler
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq


class StockCurrHandler(BaseHandler):
    # 接口：stock_zh_a_spot_em
    # 此处__download__不需要参数
    def __download__(self) -> DataFrame:
        curr_info = ak.stock_zh_a_spot_em()
        return curr_info

    def __save_to_database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        if (not isinstance(data, DataFrame)) or data.empty:
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
        table_meta = DataFrame(
            data=cols,
            columns=META_COLS)
        self.op.create_table(
            name=name,
            col_meta=table_meta)
        self.op.insert_data(
            name=name,
            data=data)

    def download_and_save(self) -> list:
        stocks = self.__download__()
        self.__save_to_database__('curr_stock_info', stocks)
