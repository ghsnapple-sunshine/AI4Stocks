import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import FuquanType, DataFreqType, META_COLS, StockCode
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq, MysqlOperator
from ai4stocks.download.slow.slow_handler_base import SlowHandlerBase


class AkStockMinuteHandler(SlowHandlerBase):
    def __init__(self, operator: MysqlOperator):
        super().__init__(operator)
        self.fuquans = [FuquanType.NONE]
        self.freq = DataFreqType.MIN5

    """
    def download_and_save(
            self,
            start_time: DateTime,
            end_time: DateTime
    ) -> list:
        return super().download_and_save(
            start_time=start_time,
            end_time=end_time
        )

    def get_table(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> DataFrame:
        return super().get_table(
            code=code,
            fuquan=fuquan
        )
    """

    def __download__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime
    ) -> DataFrame:
        # 使用接口（stock_zh_a_hist_min_em，源：东财）,code为Str6
        minute_info = ak.stock_zh_a_hist_min_em(
            symbol=code.to_code6(),
            period='5',
            start_date=start_time.format('YYYY-MM-DD HH:mm:ss'),
            end_date=end_time.format('YYYY-MM-DD HH:mm:ss'),
            adjust=fuquan.to_req())

        # 重命名
        MINUTE_NAME_DICT = {'时间': 'datetime',
                            '开盘': 'open',
                            '收盘': 'close',
                            '最高': 'high',
                            '最低': 'low',
                            '成交量': 'chengjiaoliang',
                            '成交额': 'chengjiaoe',
                            '振幅': 'zhenfu',
                            '涨跌幅': 'zhangdiefu',
                            '涨跌额': 'zhangdiee',
                            '换手率': 'huanshoulv'}
        minute_info.rename(
            columns=MINUTE_NAME_DICT,
            inplace=True)
        return minute_info

    def __save_to_database__(
            self,
            name: str,
            data: DataFrame
    ):
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
        table_meta = DataFrame(data=cols, columns=META_COLS)
        self.operator.create_table(name=name, col_meta=table_meta)
        self.operator.insert_data(name=name, data=data)
