import akshare as ak
from pandas import DataFrame

from ai4stocks.common.types import FuquanType, DataSourceType, DataFreqType
from ai4stocks.common.constants import META_COLS
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.base_handler import BaseHandler
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from pendulum import DateTime

from ai4stocks.download.download_recorder import DownloadRecorder


class StockMinuteHandler(BaseHandler):
    def __init__(self, op: MysqlOperator):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.AKSHARE_DONGCAI
        self.fuquans = [FuquanType.NONE]
        self.freq = DataFreqType.MIN5

    def Download(self, code: StockCode, fuquan: FuquanType, start_date: DateTime, end_date: DateTime) -> DataFrame:
        # 使用接口（stock_zh_a_hist_min_em，源：东财）,code为Str6
        start_date = start_date.format('YYYY-MM-DD HH:mm:ss')
        end_date = end_date.format('YYYY-MM-DD HH:mm:ss')
        minute_info = ak.stock_zh_a_hist_min_em(
            symbol=code.toCode6(), period='5', start_date=start_date, end_date=end_date, adjust=fuquan.ToReq())

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
        minute_info.rename(columns=MINUTE_NAME_DICT, inplace=True)
        return minute_info

    def Save2Database(self, name: str, data: DataFrame):
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
        self.op.CreateTable(name, table_meta)
        self.op.InsertData(name, data)
