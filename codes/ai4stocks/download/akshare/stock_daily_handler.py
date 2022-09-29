import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from ai4stocks.common.constants import META_COLS
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.base_handler import BaseHandler
from ai4stocks.download.download_recorder import DownloadRecorder
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class StockDailyHandler(BaseHandler):
    def __init__(self, op: MysqlOperator):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.AKSHARE_DONGCAI
        self.fuquans = [FuquanType.NONE, FuquanType.QIANFUQIAN, FuquanType.HOUFUQIAN]
        self.freq = DataFreqType.DAY

    def __download__(self, code: StockCode, fuquan: FuquanType, start_time: DateTime, end_time: DateTime) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_time = start_time.format('YYYYMMDD')
        end_time = end_time.format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(
            symbol=code.to_code6(),
            period='daily',
            start_date=start_time,
            end_date=end_time,
            adjust=fuquan.to_req()
        )

        # 重命名
        DAILY_NAME_DICT = {'日期': 'date',
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
        daily_info.rename(columns=DAILY_NAME_DICT, inplace=True)
        return daily_info

    def __save_to_database__(self, name: str, data: DataFrame) -> None:
        cols = [
            ['date', MysqlColType.DATE, MysqlColAddReq.KEY],
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
        self.op.create_table(name, table_meta, if_not_exist=True)
        self.op.insert_data(name, data)
