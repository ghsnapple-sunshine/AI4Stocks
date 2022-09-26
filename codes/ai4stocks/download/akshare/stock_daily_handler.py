import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.common import FuquanType, DataFreqType, DataSourceType
from ai4stocks.common.stock_code import StockCode, StockCodeType
from ai4stocks.download.base_handler import BaseHandler
from ai4stocks.download.download_recorder import DownloadRecorder
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class StockDailyHandler(BaseHandler):
    def __init__(self, op: MysqlOperator):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.AKSHARE_DONGCAI
        self.fuquans = [FuquanType.NONE, FuquanType.QIANFUQIAN, FuquanType.HOUFUQIAN]
        self.freq = DataFreqType.DAY

    def Download(self, code: StockCode, fuquan: FuquanType, start_date: DateTime, end_date: DateTime) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_date = start_date.format('YYYYMMDD')
        end_date = end_date.format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(
            symbol=code.toCode6(), period='daily', start_date=start_date, end_date=end_date, adjust=fuquan.ToReq())

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

    def Save2Database(self, name: str, data: DataFrame) -> None:
        cols = [
            ['date', MysqlColType.DATE, MysqlColAddReq.PRIMKEY],
            ['open', MysqlColType.Float, MysqlColAddReq.NONE],
            ['close', MysqlColType.Float, MysqlColAddReq.NONE],
            ['high', MysqlColType.Float, MysqlColAddReq.NONE],
            ['low', MysqlColType.Float, MysqlColAddReq.NONE],
            ['chengjiaoliang', MysqlColType.Int32, MysqlColAddReq.NONE],
            ['chengjiaoe', MysqlColType.Float, MysqlColAddReq.NONE],
            ['zhenfu', MysqlColType.Float, MysqlColAddReq.NONE],
            ['zhangdiefu', MysqlColType.Float, MysqlColAddReq.NONE],
            ['zhangdiee', MysqlColType.Float, MysqlColAddReq.NONE],
            ['huanshoulv', MysqlColType.Float, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(name, table_meta, if_not_exist=True)
        self.op.TryInsertData(name, data)
